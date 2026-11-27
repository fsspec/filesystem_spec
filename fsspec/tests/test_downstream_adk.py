"""Downstream validation tests for Google ADK FsspecArtifactService."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
import uuid
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest

pytest.importorskip("google.adk")
pytest.importorskip("google.adk.integrations.fsspec")

import requests
from google.adk.features._feature_registry import (
    FeatureName,
    temporary_feature_override,
)
from google.adk.integrations.fsspec import FsspecArtifactService, _backends
from google.genai import types
from packaging.version import Version

if TYPE_CHECKING:
    import fsspec

FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:1.54.0"
AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.37.0"
CHUNK_SIZE_BYTES = 256 * 1024


@dataclasses.dataclass(frozen=True)
class BackendHandle:
    base_url: str
    storage_options: dict[str, Any]
    make_fs: Callable[..., fsspec.AbstractFileSystem]
    supports_exclusive_create: bool
    multipart_size: int


@contextlib.contextmanager
def _docker_emulator(
    prefix: str,
    image: str,
    container_port: int,
    extra_args: list[str],
    ready_path: str = "/",
) -> Iterator[int]:
    docker = pytest.importorskip("docker")
    try:
        client = docker.from_env()
        client.ping()
    except docker.errors.DockerException as exc:
        pytest.skip(f"Docker daemon is not reachable: {exc}")

    port_spec = f"{container_port}/tcp"
    container_name = f"adk-{prefix}-{uuid.uuid4().hex[:12]}"
    container = None
    try:
        container = client.containers.run(
            image,
            extra_args,
            name=container_name,
            detach=True,
            ports={port_spec: ("127.0.0.1", None)},
        )
        container.reload()
        host_port = int(container.ports[port_spec][0]["HostPort"])
        ready_url = f"http://127.0.0.1:{host_port}{ready_path}"
        deadline = time.monotonic() + 20.0
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if requests.get(ready_url, timeout=1.0).status_code < 500:
                    break
            except requests.RequestException as exc:
                last_error = exc
            time.sleep(0.2)
        else:
            raise RuntimeError(
                f"Emulator at {ready_url} did not become ready: {last_error}"
            )
        yield host_port
    finally:
        with contextlib.suppress(docker.errors.DockerException):
            if container is not None:
                container.remove(force=True)
            else:
                client.containers.get(container_name).remove(force=True)
        client.close()


@contextlib.contextmanager
def _patch_gcs_emulator_bucket_lookup() -> Iterator[None]:
    """Skips ExtendedGcsFileSystem gRPC StorageControl lookup on HTTP emulators."""
    from gcsfs import extended_gcsfs

    ext_cls = extended_gcsfs.ExtendedGcsFileSystem
    if hasattr(ext_cls, "_get_bucket_layout"):
        with mock.patch.object(
            ext_cls,
            "_get_bucket_layout",
            return_value=extended_gcsfs.BucketLayout.UNKNOWN,
        ):
            yield
        return
    if hasattr(ext_cls, "_get_bucket_type"):
        with mock.patch.object(
            ext_cls,
            "_get_bucket_type",
            return_value=extended_gcsfs.BucketType.UNKNOWN,
        ):
            yield
        return
    yield


@pytest.fixture(autouse=True)
def _enable_adk_fsspec_feature(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("ADK_SUPPRESS_EXPERIMENTAL_FEATURE_WARNINGS", "1")
    monkeypatch.setenv("NO_GCE_CHECK", "true")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "adk-fsspec-test-project")
    with temporary_feature_override(FeatureName.FSSPEC_ARTIFACT_SERVICE, True):
        yield


@pytest.fixture(scope="session")
def _s3_emulator() -> Iterator[tuple[str, dict[str, Any], bool]]:
    s3fs = pytest.importorskip("s3fs")
    moto = pytest.importorskip("moto")
    moto_server_mod = pytest.importorskip("moto.moto_server.threaded_moto_server")

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AWS_ACCESS_KEY_ID", "adk-test-key")
        mp.setenv("AWS_SECRET_ACCESS_KEY", "adk-test-secret")
        mp.delenv("AWS_SECURITY_TOKEN", raising=False)
        mp.delenv("AWS_SESSION_TOKEN", raising=False)
        mp.setenv("AWS_DEFAULT_REGION", "us-east-1")

        server = moto_server_mod.ThreadedMotoServer(
            ip_address="127.0.0.1", port=0, verbose=False
        )
        server.start()
        try:
            port = (
                server.get_host_and_port()[1]
                if hasattr(server, "get_host_and_port")
                else server._server.server_port
            )
            storage_options: dict[str, Any] = {
                "anon": False,
                "key": "adk-test-key",
                "secret": "adk-test-secret",
                "client_kwargs": {"endpoint_url": f"http://127.0.0.1:{port}"},
            }
            bucket_name = f"adk-s3-{uuid.uuid4().hex[:8]}"
            setup_fs = s3fs.S3FileSystem(skip_instance_cache=True, **storage_options)
            setup_fs.mkdir(bucket_name)
            supports_exclusive = Version(moto.__version__) >= Version("5.1")
            yield f"s3://{bucket_name}", storage_options, supports_exclusive
        finally:
            server.stop()


@pytest.fixture(scope="session")
def _gcs_emulator() -> Iterator[tuple[str, dict[str, Any]]]:
    gcsfs = pytest.importorskip("gcsfs")
    args = ["-scheme", "http", "-public-host", "0.0.0.0:4443", "-backend", "memory"]
    with _docker_emulator(
        "gcs", FAKE_GCS_IMAGE, 4443, args, "/storage/v1/b"
    ) as host_port:
        endpoint_url = f"http://127.0.0.1:{host_port}"
        requests.put(
            f"{endpoint_url}/_internal/config",
            json={"externalUrl": endpoint_url},
            timeout=5.0,
        ).raise_for_status()
        storage_options: dict[str, Any] = {
            "token": "anon",
            "endpoint_url": endpoint_url,
            "default_block_size": CHUNK_SIZE_BYTES,
        }
        bucket_name = f"adk-gcs-{uuid.uuid4().hex[:8]}"
        with _patch_gcs_emulator_bucket_lookup():
            setup_fs = gcsfs.GCSFileSystem(skip_instance_cache=True, **storage_options)
            setup_fs.mkdir(bucket_name)
        yield f"gs://{bucket_name}", storage_options


@pytest.fixture(scope="session")
def _azure_emulator() -> Iterator[tuple[str, dict[str, Any]]]:
    adlfs = pytest.importorskip("adlfs")
    if Version(adlfs.__version__) < Version("2026.8.0"):
        pytest.skip(f"Requires adlfs>=2026.8.0, got {adlfs.__version__}")
    if not hasattr(_backends, "_AzureBackendAdapter"):
        pytest.skip("Installed google-adk predates _AzureBackendAdapter.")

    from adlfs.tests.constants import ACCOUNT_NAME, KEY

    args = [
        "azurite-blob",
        "--blobHost",
        "0.0.0.0",
        "--skipApiVersionCheck",
        "--loose",
    ]
    with _docker_emulator("azure", AZURITE_IMAGE, 10000, args) as host_port:
        connection_string = (
            "DefaultEndpointsProtocol=http;"
            f"AccountName={ACCOUNT_NAME};"
            f"AccountKey={KEY};"
            f"BlobEndpoint=http://127.0.0.1:{host_port}/{ACCOUNT_NAME};"
        )
        storage_options: dict[str, Any] = {
            "connection_string": connection_string,
            "blocksize": CHUNK_SIZE_BYTES,
        }
        blob_container = f"adk-azure-{uuid.uuid4().hex[:8]}"
        setup_fs = adlfs.AzureBlobFileSystem(
            skip_instance_cache=True, **storage_options
        )
        setup_fs.mkdir(blob_container)
        yield f"abfs://{blob_container}", storage_options


@pytest.fixture(params=["s3", "gcs", "azure"])
def cloud_backend(request: pytest.FixtureRequest) -> Iterator[BackendHandle]:
    # fsspec/conftest.py's autouse instance_caches fixture clears _Cached before/after.
    backend_name: str = request.param
    run_prefix = f"run-{uuid.uuid4().hex[:10]}"

    if backend_name == "s3":
        root_uri, storage_options, supports_exclusive = request.getfixturevalue(
            "_s3_emulator"
        )
        s3fs = pytest.importorskip("s3fs")
        yield BackendHandle(
            base_url=f"{root_uri}/{run_prefix}",
            storage_options=dict(storage_options),
            make_fs=lambda **kw: s3fs.S3FileSystem(**storage_options, **kw),
            supports_exclusive_create=supports_exclusive,
            multipart_size=6 * 1024 * 1024,
        )
        return

    if backend_name == "gcs":
        root_uri, storage_options = request.getfixturevalue("_gcs_emulator")
        gcsfs = pytest.importorskip("gcsfs")
        with _patch_gcs_emulator_bucket_lookup():
            yield BackendHandle(
                base_url=f"{root_uri}/{run_prefix}",
                storage_options=dict(storage_options),
                make_fs=lambda **kw: gcsfs.GCSFileSystem(**storage_options, **kw),
                supports_exclusive_create=True,
                multipart_size=300 * 1024,
            )
        return

    root_uri, storage_options = request.getfixturevalue("_azure_emulator")
    adlfs = pytest.importorskip("adlfs")
    yield BackendHandle(
        base_url=f"{root_uri}/{run_prefix}",
        storage_options=dict(storage_options),
        make_fs=lambda **kw: adlfs.AzureBlobFileSystem(**storage_options, **kw),
        supports_exclusive_create=True,
        multipart_size=300 * 1024,
    )


def _build_service(backend: BackendHandle, mode: str) -> FsspecArtifactService:
    if mode == "uri_mode":
        return FsspecArtifactService(backend.base_url, **backend.storage_options)
    backend_fs = backend.make_fs(skip_instance_cache=True, use_listings_cache=True)
    return FsspecArtifactService(backend.base_url, fs=backend_fs)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["uri_mode", "fs_mode"])
async def test_crud_versioning_and_multipart(
    cloud_backend: BackendHandle, mode: str
) -> None:
    service = _build_service(cloud_backend, mode)
    assert service._fs.dircache.use_listings_cache is (mode == "fs_mode")

    scope = {"app_name": "adk-app", "user_id": "alice", "session_id": "sess-1"}
    assert await service.load_artifact(**scope, filename="weights/model.bin") is None
    assert (
        await service.get_artifact_version(**scope, filename="weights/model.bin")
        is None
    )

    small_payload = b"initial-binary-payload-v0"
    large_payload = b"M" * cloud_backend.multipart_size + b"-multipart-tail-v1"

    v0 = await service.save_artifact(
        **scope,
        filename="weights/model.bin",
        artifact=types.Part(
            inline_data=types.Blob(
                data=small_payload, mime_type="application/x-adk-weights"
            )
        ),
        custom_metadata={"traceid": "trace-001", "stage": "initial"},
    )
    v1 = await service.save_artifact(
        **scope,
        filename="weights/model.bin",
        artifact=types.Part(
            inline_data=types.Blob(
                data=large_payload, mime_type="application/octet-stream"
            )
        ),
        custom_metadata={"traceid": "trace-002", "stage": "multipart"},
    )
    assert (v0, v1) == (0, 1)
    assert await service.list_artifact_keys(**scope) == ["weights/model.bin"]
    assert await service.list_versions(**scope, filename="weights/model.bin") == [0, 1]

    loaded_latest = await service.load_artifact(**scope, filename="weights/model.bin")
    assert loaded_latest is not None and loaded_latest.inline_data is not None
    assert loaded_latest.inline_data.data == large_payload

    loaded_v0 = await service.load_artifact(
        **scope, filename="weights/model.bin", version=0
    )
    assert loaded_v0 is not None and loaded_v0.inline_data is not None
    assert loaded_v0.inline_data.data == small_payload
    assert loaded_v0.inline_data.mime_type == "application/x-adk-weights"

    meta_v0 = await service.get_artifact_version(
        **scope, filename="weights/model.bin", version=0
    )
    assert meta_v0 is not None
    assert meta_v0.canonical_uri.endswith("/adk-app/alice/sess-1/weights/model.bin/0")
    assert meta_v0.custom_metadata == {"traceid": "trace-001", "stage": "initial"}

    all_versions = await service.list_artifact_versions(
        **scope, filename="weights/model.bin"
    )
    assert [item.version for item in all_versions] == [0, 1]
    assert all_versions[0].create_time > 0
    assert all_versions[0].custom_metadata == {
        "traceid": "trace-001",
        "stage": "initial",
    }
    assert all_versions[1].mime_type == "application/octet-stream"
    assert all_versions[1].custom_metadata == {
        "traceid": "trace-002",
        "stage": "multipart",
    }

    await service.delete_artifact(**scope, filename="weights/model.bin")
    assert await service.load_artifact(**scope, filename="weights/model.bin") is None
    assert await service.list_versions(**scope, filename="weights/model.bin") == []
    assert await service.list_artifact_keys(**scope) == []


@pytest.mark.asyncio
async def test_text_refs_and_user_scope(cloud_backend: BackendHandle) -> None:
    service = _build_service(cloud_backend, "fs_mode")
    sess_one = {"app_name": "adk-app", "user_id": "bob", "session_id": "sess-1"}
    sess_two = {"app_name": "adk-app", "user_id": "bob", "session_id": "sess-2"}

    unicode_text = "Summary line \u4f60\u597d with 100% fidelity"
    await service.save_artifact(
        **sess_one, filename="summary.txt", artifact=types.Part(text=unicode_text)
    )
    loaded_text = await service.load_artifact(**sess_one, filename="summary.txt")
    assert loaded_text is not None and loaded_text.text == unicode_text

    unicode_display = "Chart 100% \u4f60\u597d v2.png"
    png_bytes = b"\x89PNG\r\n\x1a\npayload"
    await service.save_artifact(
        **sess_one,
        filename="user:chart.png",
        artifact=types.Part(
            inline_data=types.Blob(
                data=png_bytes, mime_type="image/png", display_name=unicode_display
            )
        ),
    )
    cross_session = await service.load_artifact(**sess_two, filename="user:chart.png")
    assert cross_session is not None and cross_session.inline_data is not None
    assert cross_session.inline_data.display_name == unicode_display
    assert await service.list_artifact_keys(**sess_two) == ["user:chart.png"]
    assert await service.list_artifact_keys(app_name="adk-app", user_id="bob") == [
        "user:chart.png"
    ]

    external_uri = "https://example.com/docs/spec%20v1.pdf"
    await service.save_artifact(
        **sess_one,
        filename="spec_ref.pdf",
        artifact=types.Part(
            file_data=types.FileData(file_uri=external_uri, mime_type="application/pdf")
        ),
    )
    loaded_ext = await service.load_artifact(**sess_one, filename="spec_ref.pdf")
    assert loaded_ext is not None and loaded_ext.file_data is not None
    assert loaded_ext.file_data.file_uri == external_uri
    assert loaded_ext.file_data.mime_type == "application/pdf"

    await service.save_artifact(
        **sess_two,
        filename="alias_chart",
        artifact=types.Part(
            file_data=types.FileData(
                file_uri=(
                    "artifact://apps/adk-app/users/bob/"
                    "artifacts/user:chart.png/versions/0"
                ),
                mime_type="image/png",
            )
        ),
    )
    resolved = await service.load_artifact(**sess_two, filename="alias_chart")
    assert resolved is not None and resolved.inline_data is not None
    assert resolved.inline_data.data == png_bytes


@pytest.mark.asyncio
async def test_prefix_disambiguation_and_dircache_invalidation(
    cloud_backend: BackendHandle,
) -> None:
    reader_fs = cloud_backend.make_fs(skip_instance_cache=True, use_listings_cache=True)
    writer_fs = cloud_backend.make_fs(skip_instance_cache=True, use_listings_cache=True)
    reader = FsspecArtifactService(cloud_backend.base_url, fs=reader_fs)
    writer = FsspecArtifactService(cloud_backend.base_url, fs=writer_fs)
    scope = {"app_name": "adk-app", "user_id": "carol", "session_id": "sess-cache"}

    await reader.save_artifact(
        **scope,
        filename="report",
        artifact=types.Part(
            inline_data=types.Blob(data=b"parent-v0", mime_type="text/plain")
        ),
    )
    assert await reader.list_artifact_keys(**scope) == ["report"]
    assert await reader.list_versions(**scope, filename="report") == [0]
    primed = await reader.load_artifact(**scope, filename="report")
    assert primed is not None and primed.inline_data is not None
    assert primed.inline_data.data == b"parent-v0"

    await writer.save_artifact(
        **scope,
        filename="report/0",
        artifact=types.Part(
            inline_data=types.Blob(data=b"nested-zero-v0", mime_type="text/plain")
        ),
    )
    await writer.save_artifact(
        **scope,
        filename="report",
        artifact=types.Part(
            inline_data=types.Blob(data=b"parent-v1", mime_type="text/plain")
        ),
    )

    explicit_v1 = await reader.load_artifact(**scope, filename="report", version=1)
    assert explicit_v1 is not None and explicit_v1.inline_data is not None
    assert explicit_v1.inline_data.data == b"parent-v1"

    assert await reader.list_versions(**scope, filename="report") == [0, 1]
    await writer.save_artifact(
        **scope,
        filename="report",
        artifact=types.Part(
            inline_data=types.Blob(data=b"parent-v2", mime_type="text/plain")
        ),
    )

    latest = await reader.load_artifact(**scope, filename="report")
    nested = await reader.load_artifact(**scope, filename="report/0")
    assert latest is not None and latest.inline_data is not None
    assert nested is not None and nested.inline_data is not None
    assert latest.inline_data.data == b"parent-v2"
    assert nested.inline_data.data == b"nested-zero-v0"
    assert await reader.list_artifact_keys(**scope) == ["report", "report/0"]


@pytest.mark.asyncio
async def test_concurrent_save_contention(cloud_backend: BackendHandle) -> None:
    if not cloud_backend.supports_exclusive_create:
        pytest.skip("Moto < 5.1 does not enforce If-None-Match: * on S3 PutObject.")

    service = _build_service(cloud_backend, "fs_mode")
    scope = {"app_name": "adk-app", "user_id": "dave", "session_id": "sess-race"}
    worker_count = 8
    assigned_versions = await asyncio.gather(
        *[
            service.save_artifact(
                **scope,
                filename="contended.bin",
                artifact=types.Part(
                    inline_data=types.Blob(
                        data=f"payload-{worker_idx}".encode(),
                        mime_type="application/octet-stream",
                    )
                ),
            )
            for worker_idx in range(worker_count)
        ]
    )
    assert sorted(assigned_versions) == list(range(worker_count))

    observed = set()
    for version_num in range(worker_count):
        loaded = await service.load_artifact(
            **scope, filename="contended.bin", version=version_num
        )
        assert loaded is not None and loaded.inline_data is not None
        observed.add(loaded.inline_data.data)
    assert observed == {f"payload-{idx}".encode() for idx in range(worker_count)}

    empty_workers = 4
    empty_versions = await asyncio.gather(
        *[
            service.save_artifact(
                **scope,
                filename="empty_contended.txt",
                artifact=types.Part(
                    inline_data=types.Blob(data=b"", mime_type="text/plain")
                ),
                custom_metadata={"worker": str(worker_idx)},
            )
            for worker_idx in range(empty_workers)
        ]
    )
    assert sorted(empty_versions) == list(range(empty_workers))
    empty_metas = await service.list_artifact_versions(
        **scope, filename="empty_contended.txt"
    )
    assert {item.custom_metadata["worker"] for item in empty_metas} == {
        str(idx) for idx in range(empty_workers)
    }
    loaded_empty = await service.load_artifact(
        **scope, filename="empty_contended.txt", version=0
    )
    assert loaded_empty is not None and loaded_empty.inline_data is not None
    assert loaded_empty.inline_data.data == b""
