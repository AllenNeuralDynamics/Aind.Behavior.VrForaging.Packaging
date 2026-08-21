"""Unit tests for ``S3InputStore``'s client construction — no network.

The one thing worth pinning here: which of three credential paths a given
constructor call takes, since an unsigned client silently returns AccessDenied
rather than a wrong-config error, and an explicit ``client=`` must always win over
either default.
"""

from typing import cast
from unittest.mock import patch

from botocore.client import BaseClient

from processing_server.stores.input_s3 import S3InputStore


class TestClientConstruction:
    def test_defaults_to_ambient_credentials(self):
        """The historical default: an instance role / profile / env, not anonymous —
        most raw sessions are in a private bucket."""
        with patch("processing_server.stores.input_s3.boto3.client") as mock_client:
            S3InputStore()
        mock_client.assert_called_once_with("s3")

    def test_anonymous_requests_an_unsigned_client(self):
        with patch("processing_server.stores.input_s3.boto3.client") as mock_client:
            S3InputStore(anonymous=True)
        (_, kwargs) = mock_client.call_args
        assert mock_client.call_args[0] == ("s3",)
        assert "config" in kwargs

    def test_an_explicit_client_wins_over_anonymous(self):
        """A caller-supplied client (tests, or a future signed-URL story) must not be
        silently replaced by the anonymous default."""
        sentinel = cast(BaseClient, object())
        with patch("processing_server.stores.input_s3.boto3.client") as mock_client:
            store = S3InputStore(client=sentinel, anonymous=True)
        mock_client.assert_not_called()
        assert store._client is sentinel

    def test_an_explicit_client_wins_over_the_ambient_default(self):
        sentinel = cast(BaseClient, object())
        with patch("processing_server.stores.input_s3.boto3.client") as mock_client:
            store = S3InputStore(client=sentinel)
        mock_client.assert_not_called()
        assert store._client is sentinel


class TestWorkerThreadsAnonymousThrough:
    def test_input_store_kwargs_carries_anonymous_for_s3(self, tmp_path):
        from processing_server.config import PipelineConfig
        from processing_server.stores.output_local import LocalOutputStore
        from processing_server.worker import Worker

        config = PipelineConfig(
            release="rel1",
            ingestion={"type": "local", "root": str(tmp_path)},
            input={"store": "s3", "anonymous": True},
            output={"store": "local", "uri": str(tmp_path / "out")},
            worker={"ledger": str(tmp_path / "jobs.sqlite")},
            processor={"allow_unpinned": True},
            logging={"dir": str(tmp_path / "logs")},
        )
        worker = Worker(
            config,
            worker_id="w1",
            work_dir=tmp_path / "work",
            output_store=LocalOutputStore(),
        )
        try:
            assert worker._input_store_kwargs() == {"anonymous": True}
        finally:
            worker.close()

    def test_defaults_to_false_when_unset(self, tmp_path):
        from processing_server.config import PipelineConfig
        from processing_server.stores.output_local import LocalOutputStore
        from processing_server.worker import Worker

        config = PipelineConfig(
            release="rel1",
            ingestion={"type": "local", "root": str(tmp_path)},
            input={"store": "s3"},
            output={"store": "local", "uri": str(tmp_path / "out")},
            worker={"ledger": str(tmp_path / "jobs.sqlite")},
            processor={"allow_unpinned": True},
            logging={"dir": str(tmp_path / "logs")},
        )
        worker = Worker(
            config,
            worker_id="w1",
            work_dir=tmp_path / "work",
            output_store=LocalOutputStore(),
        )
        try:
            assert worker._input_store_kwargs() == {"anonymous": False}
        finally:
            worker.close()
