#!/usr/bin/env python3
"""Keep Linux CI ephemeral sockets out of the journal's explicit bind range."""

import json
import os
import re
import subprocess
import sys


KEY = 'net.ipv4.ip_local_reserved_ports'
JOURNAL_PORTS = set(range(49_152, 65_536))


def parse_ports(value):
    ports = set()
    if not value.strip():
        return ports
    for item in value.strip().split(','):
        item = item.strip()
        if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?', item):
            raise ValueError('Invalid existing reserved-port range')
        bounds = [int(part) for part in item.split('-')]
        first, last = bounds[0], bounds[-1]
        if not 0 <= first <= last <= 65_535:
            raise ValueError('Reserved ports must be within 0-65535')
        ports.update(range(first, last + 1))
    return ports


def format_ports(ports):
    ranges = []
    for port in sorted(ports):
        if ranges and port == ranges[-1][1] + 1:
            ranges[-1][1] = port
        else:
            ranges.append([port, port])
    return ','.join(str(first) if first == last else f'{first}-{last}' for first, last in ranges)


def read_ports():
    result = subprocess.run(['sysctl', '-n', KEY], check=True, capture_output=True,
                            text=True, timeout=10)
    return parse_ports(result.stdout)


def reserve_journal_ports():
    if sys.platform != 'linux' or os.environ.get('GITHUB_ACTIONS') != 'true':
        raise RuntimeError('Journal port reservation is restricted to Linux GitHub Actions')
    before = read_ports()
    expected = before | JOURNAL_PORTS
    if expected != before:
        subprocess.run(['sudo', 'sysctl', '-w', f'{KEY}={format_ports(expected)}'],
                       check=True, capture_output=True, text=True, timeout=10)
    actual = read_ports()
    if not expected <= actual:
        raise RuntimeError('Reserved-port readback lost existing or required journal reservations')
    return {'journal_ports': '49152-65535', 'reserved_ports': format_ports(actual)}


if __name__ == '__main__':
    print(json.dumps(reserve_journal_ports()))
