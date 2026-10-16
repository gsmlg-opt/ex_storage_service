import os
import subprocess
import unittest
from unittest.mock import call, patch

import reserve_journal_ports as reservation


class ReservedPortRangesTest(unittest.TestCase):
    def test_existing_disjoint_overlapping_and_adjacent_reservations_are_preserved(self):
        original = reservation.parse_ports('100,200-203,202-205,206,60000-60002\n')
        combined = original | reservation.JOURNAL_PORTS
        self.assertEqual(reservation.format_ports(combined), '100,200-206,49152-65535')
        self.assertEqual(reservation.parse_ports(reservation.format_ports(combined)), combined)
        self.assertTrue(original <= combined)

    def test_empty_and_complete_port_ranges_roundtrip(self):
        self.assertEqual(reservation.parse_ports(' \n'), set())
        self.assertEqual(reservation.format_ports(set()), '')
        complete = reservation.parse_ports('0-65535')
        self.assertEqual(len(complete), 65_536)
        self.assertEqual(reservation.format_ports(complete), '0-65535')

    def test_malformed_or_out_of_range_values_are_rejected(self):
        for value in ('1,,2', ',1', '1,', '-1', '65536', '20-10', '1-65536', '1-2-3', '1;echo bad'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                reservation.parse_ports(value)


class JournalPortReservationTest(unittest.TestCase):
    def setUp(self):
        self.addCleanup(patch.stopall)
        patch.object(reservation.sys, 'platform', 'linux').start()
        patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}).start()
        self.run = patch.object(reservation.subprocess, 'run').start()

    def read_result(self, value):
        return subprocess.CompletedProcess(['sysctl'], 0, stdout=value, stderr='')

    def test_unions_reservations_and_uses_literal_sudo_sysctl_arguments(self):
        self.run.side_effect = [self.read_result('100,40000-40002'), self.read_result(''),
                                self.read_result('49152-65535,100,40002,40000-40001')]
        result = reservation.reserve_journal_ports()
        options = {'check': True, 'capture_output': True, 'text': True, 'timeout': 10}
        self.assertEqual(self.run.call_args_list, [
            call(['sysctl', '-n', reservation.KEY], **options),
            call(['sudo', 'sysctl', '-w', f'{reservation.KEY}=100,40000-40002,49152-65535'], **options),
            call(['sysctl', '-n', reservation.KEY], **options),
        ])
        self.assertEqual(result, {'journal_ports': '49152-65535',
                                  'reserved_ports': '100,40000-40002,49152-65535'})

    def test_empty_initial_reservations_receive_the_entire_journal_range(self):
        self.run.side_effect = [self.read_result(''), self.read_result(''), self.read_result('49152-65535')]
        reservation.reserve_journal_ports()
        self.assertEqual(self.run.call_args_list[1].args[0],
                         ['sudo', 'sysctl', '-w', f'{reservation.KEY}=49152-65535'])

    def test_already_reserved_range_requires_no_mutation(self):
        self.run.side_effect = [self.read_result('100,49000-65535'), self.read_result('100,49000-65535')]
        reservation.reserve_journal_ports()
        self.assertEqual(self.run.call_count, 2)
        self.assertTrue(all(item.args[0][0] == 'sysctl' for item in self.run.call_args_list))

    def test_readback_may_add_reservations_but_must_retain_every_existing_port(self):
        for actual, valid in [('100,200,49152-65535', True), ('49152-65535', False),
                              ('100,49152-65534', False)]:
            with self.subTest(actual=actual):
                self.run.side_effect = [self.read_result('100'), self.read_result(''), self.read_result(actual)]
                if valid:
                    self.assertEqual(reservation.reserve_journal_ports()['reserved_ports'], actual)
                else:
                    with self.assertRaisesRegex(RuntimeError, 'readback lost'):
                        reservation.reserve_journal_ports()

    def test_workstation_and_non_linux_hosts_cannot_even_read_sysctl(self):
        for platform, environment in [('linux', ''), ('linux', 'false'), ('linux', 'True'),
                                       ('darwin', 'true'), ('win32', 'true')]:
            with self.subTest(platform=platform, environment=environment), \
                    patch.object(reservation.sys, 'platform', platform), \
                    patch.dict(os.environ, {'GITHUB_ACTIONS': environment}), \
                    self.assertRaisesRegex(RuntimeError, 'restricted to Linux GitHub Actions'):
                reservation.reserve_journal_ports()
        self.run.assert_not_called()

    def test_invalid_existing_value_fails_before_mutation(self):
        self.run.return_value = self.read_result('100-50')
        with self.assertRaises(ValueError):
            reservation.reserve_journal_ports()
        self.assertEqual(self.run.call_count, 1)

    def test_sysctl_failure_is_not_ignored_or_followed_by_another_write(self):
        error = subprocess.CalledProcessError(1, ['sudo', 'sysctl'])
        self.run.side_effect = [self.read_result('100'), error]
        with self.assertRaises(subprocess.CalledProcessError):
            reservation.reserve_journal_ports()
        self.assertEqual(self.run.call_count, 2)


if __name__ == '__main__':
    unittest.main()
