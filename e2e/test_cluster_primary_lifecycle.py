"""Role-aware outage assertions must never turn a data loss into strict-write success."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
import cluster_e2e
import s3_hardening_resilience as resilience


def failure(status):
    return ClientError({"ResponseMetadata": {"HTTPStatusCode": status},
                        "Error": {"Code": str(status)}}, "PutObject")


class PrimaryOutageTest(unittest.TestCase):
    def test_cluster_checks_only_live_data_and_requires_role_correct_write_status(self):
        for primary in ("meta", "data-a", "data-b"):
            with self.subTest(primary=primary), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                topology = Path(directory) / "primary-loss.json"
                payload = b"acknowledged"
                state = {"bucket": "test", "persistent_key": "old",
                         "persistent_sha256": cluster_e2e.hashlib.sha256(payload).hexdigest()}
                path.write_text(json.dumps(state))
                topology.write_text(json.dumps({"before": {"primary": primary, "view": 1},
                                               "elected": {"view": 2}}))
                peers = [MagicMock(), MagicMock()]
                for peer in peers:
                    peer.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(
                        payload if kw["Key"] == "old" else b"quorum restored")}
                    if primary != "meta":
                        peer.put_object.side_effect = failure(503)
                with patch.object(cluster_e2e, "client", side_effect=peers), \
                        patch.object(cluster_e2e, "endpoints", return_value=["a", "b"]):
                    cluster_e2e.primary_failure(str(path), str(topology))
                if primary != "meta":
                    stopped = peers[0 if primary == "data-a" else 1]
                    stopped.get_object.assert_not_called()
                    stopped.put_object.assert_not_called()
                self.assertEqual(json.loads(path.read_text())["primary_loss"]["before"]["primary"], primary)

    def test_cluster_rejects_successful_write_during_data_primary_outage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            topology = Path(directory) / "primary-loss.json"
            path.write_text(json.dumps({"bucket": "test", "persistent_key": "old",
                "persistent_sha256": cluster_e2e.hashlib.sha256(b"old").hexdigest()}))
            topology.write_text(json.dumps({"before": {"primary": "data-a", "view": 1},
                                           "elected": {"view": 2}}))
            peer = MagicMock()
            peer.get_object.return_value = {"Body": io.BytesIO(b"old")}
            with patch.object(cluster_e2e, "client", return_value=peer), \
                    patch.object(cluster_e2e, "endpoints", return_value=["a", "b"]), \
                    self.assertRaisesRegex(AssertionError, "strict W=2 write succeeded"):
                cluster_e2e.primary_failure(str(path), str(topology))

    def test_resilience_records_actual_primary_and_preserves_strict_write_outcome(self):
        for primary in ("meta", "data-a", "data-b"):
            with self.subTest(primary=primary), tempfile.TemporaryDirectory() as directory:
                state = {"bucket": "test", "before_loss": {"primary": primary},
                         "multipart_version": "v1"}
                peers = [MagicMock(), MagicMock()]
                for peer in peers:
                    if primary != "meta":
                        peer.put_object.side_effect = failure(503)
                        peer.head_object.side_effect = failure(404)
                with patch.object(resilience, "await_topology", return_value={"view": 3, "commit": 10}), \
                        patch.object(resilience, "assert_fixture"), patch.object(resilience, "assert_get"):
                    result = resilience.run("elected", Path(directory) / "state.json", state, peers)
                self.assertEqual(result["strict_write_status"], 200 if primary == "meta" else 503)
                self.assertEqual(state["election"], {"view": 3, "commit": 10})
                if primary != "meta":
                    peers[0 if primary == "data-a" else 1].put_object.assert_not_called()

    def test_resilience_rejects_successful_write_during_data_primary_outage(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(resilience, "await_topology", return_value={"view": 3}), \
                self.assertRaises(AssertionError):
            resilience.run("elected", Path(directory) / "state.json",
                {"bucket": "test", "before_loss": {"primary": "data-b"}},
                [MagicMock(), MagicMock()])


if __name__ == "__main__":
    unittest.main()
