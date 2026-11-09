"""
Autor: Isacc Cabrera
Descripción: Pruebas del data lake sin red: LocalLakeStore en tmp_path (idempotencia por tamaño,
    llaves con "/"), la fábrica make_lake_store, S3LakeStore con un cliente S3 falso, la
    exclusión de viajes crudos y del heartbeat abierto, la sincronización en dos pasadas y la
    preparación de SeaweedFS (relleno de .env, identidades y prueba de mínimo privilegio).
Patrón: N/A (pruebas)
Fecha de creación: 2026-11-09
Cambios:
    - 2026-11-09 · Isacc Cabrera · Creación (paso 6).
"""

import io
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from radar.config import Settings
from radar.lake.s3_setup import check_policy, ensure_buckets, fill_blank_keys, s3_config
from radar.lake.store import (
    LocalLakeStore,
    S3LakeStore,
    layer_files,
    make_lake_store,
    sync_files,
)

NOW = datetime(2026, 11, 9, 21, 0, tzinfo=UTC)


def _write(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _err(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


class FakeS3:
    """Imita lo que usamos de un cliente boto3 de S3, con permisos configurables."""

    def __init__(self, allowed_buckets: set[str] | None = None, admin: bool = False) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.buckets: set[str] = set(allowed_buckets or set())
        self.allowed = allowed_buckets
        self.admin = admin
        self.uploads: list[str] = []

    def _guard(self, bucket: str) -> None:
        if not self.admin and self.allowed is not None and bucket not in self.allowed:
            raise _err("AccessDenied")

    def head_object(self, Bucket: str, Key: str) -> dict[str, int]:  # noqa: N803
        self._guard(Bucket)
        if (Bucket, Key) not in self.objects:
            raise _err("404")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self._guard(bucket)
        self.objects[(bucket, key)] = Path(filename).read_bytes()
        self.uploads.append(key)

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803
        self._guard(Bucket)
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        self._guard(Bucket)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def list_objects_v2(self, Bucket: str, Prefix: str = "") -> dict[str, Any]:  # noqa: N803
        self._guard(Bucket)
        keys = sorted(k for b, k in self.objects if b == Bucket and k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys]}

    def get_paginator(self, _name: str) -> Any:
        fake = self

        class _Pag:
            def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:  # noqa: N803
                return [fake.list_objects_v2(Bucket=Bucket, Prefix=Prefix)]

        return _Pag()

    def delete_object(self, Bucket: str, Key: str) -> None:  # noqa: N803
        self._guard(Bucket)
        self.objects.pop((Bucket, Key), None)

    def delete_bucket(self, Bucket: str) -> None:  # noqa: N803
        if not self.admin:
            raise _err("AccessDenied")
        if any(b == Bucket for b, _ in self.objects):
            raise _err("BucketNotEmpty")
        self.buckets.discard(Bucket)

    def create_bucket(self, Bucket: str) -> None:  # noqa: N803
        if not self.admin:
            raise _err("AccessDenied")
        self.buckets.add(Bucket)

    def list_buckets(self) -> dict[str, Any]:
        return {"Buckets": [{"Name": b} for b in sorted(self.buckets)]}


def test_local_put_file_segunda_vez_mismo_tamano_es_omitido(tmp_path: Path) -> None:
    src = _write(tmp_path / "src.json.gz", b"abc")
    store = LocalLakeStore(tmp_path / "lake")
    assert store.put_file(src, "dt=2026-11-08/hour=18/a.json.gz") == "subido"
    assert store.put_file(src, "dt=2026-11-08/hour=18/a.json.gz") == "omitido"
    assert store.exists("dt=2026-11-08/hour=18/a.json.gz")
    assert (tmp_path / "lake/dt=2026-11-08/hour=18/a.json.gz").read_bytes() == b"abc"


def test_local_tamano_distinto_se_reemplaza(tmp_path: Path) -> None:
    store = LocalLakeStore(tmp_path / "lake")
    store.put_file(_write(tmp_path / "v1", b"abc"), "x/part-0.parquet")
    assert store.put_file(_write(tmp_path / "v2", b"abcdef"), "x/part-0.parquet") == "subido"
    assert (tmp_path / "lake/x/part-0.parquet").read_bytes() == b"abcdef"


def test_local_list_keys_usa_barras_y_filtra_prefijo(tmp_path: Path) -> None:
    store = LocalLakeStore(tmp_path / "lake")
    src = _write(tmp_path / "s", b"1")
    store.put_file(src, "a/1.txt")
    store.put_file(src, "b/2.txt")
    assert store.list_keys() == ["a/1.txt", "b/2.txt"]
    assert store.list_keys("b/") == ["b/2.txt"]
    assert not store.exists("c/3.txt")


def test_fabrica_regresa_el_adaptador_del_backend(settings: Settings) -> None:
    local = make_lake_store(settings, "bronze", "local")
    assert isinstance(local, LocalLakeStore)
    assert local.root == settings.data_dir / "bronze"
    s3 = make_lake_store(settings, "silver", "s3", client=FakeS3())
    assert isinstance(s3, S3LakeStore)
    assert s3.bucket == "radar-silver"
    with pytest.raises(ValueError):
        make_lake_store(settings, "gold", "ftp")  # type: ignore[arg-type]


def test_s3_put_file_sube_y_luego_omite(tmp_path: Path) -> None:
    client = FakeS3()
    store = S3LakeStore("radar-bronze", client)
    src = _write(tmp_path / "f.gz", b"hola")
    assert store.put_file(src, "k/f.gz") == "subido"
    assert store.put_file(src, "k/f.gz") == "omitido"
    assert client.uploads == ["k/f.gz"]
    assert store.list_keys("k/") == ["k/f.gz"]


def test_layer_files_excluye_viajes_crudos_tmp_y_heartbeat_abierto(settings: Settings) -> None:
    bronze = settings.data_dir / "bronze"
    _write(bronze / "station_status/dt=2026-11-09/hour=20/station_status_1.json.gz", b"x")
    _write(bronze / "station_status/dt=2026-11-09/hour=20/station_status_2.json.gz.tmp", b"x")
    _write(bronze / "trips/2026-10.csv", b"Genero_Usuario,Edad_Usuario,Bici")
    _write(bronze / "heartbeat/dt=2026-11-08.jsonl", b"{}")
    _write(bronze / "heartbeat/dt=2026-11-09.jsonl", b"{}")
    keys = layer_files(settings, "bronze", now=NOW)
    assert keys == [
        "heartbeat/dt=2026-11-08.jsonl",
        "station_status/dt=2026-11-09/hour=20/station_status_1.json.gz",
    ]


def test_sync_segunda_pasada_no_resube_nada(settings: Settings) -> None:
    bronze = settings.data_dir / "bronze"
    _write(bronze / "station_status/dt=2026-11-09/hour=20/a.json.gz", b"a")
    _write(bronze / "trips/crudo.csv", b"pii")
    store = make_lake_store(settings, "bronze", "s3", client=FakeS3())
    first = sync_files(store, bronze, layer_files(settings, "bronze", now=NOW))
    second = sync_files(store, bronze, layer_files(settings, "bronze", now=NOW))
    assert first.resumen() == {"subidos": 1, "omitidos": 0, "claves_trips": 0}
    assert second.resumen() == {"subidos": 0, "omitidos": 1, "claves_trips": 0}


def test_fill_blank_keys_solo_toca_las_llaves_vacias() -> None:
    env = "LOG_LEVEL=INFO\nS3_ADMIN_ACCESS_KEY=\nS3_ADMIN_SECRET_KEY=ya\nS3_APP_ACCESS_KEY=\n"
    tokens = iter(["t1", "t2", "t3"])
    text, filled = fill_blank_keys(env, token=lambda: next(tokens))
    assert filled == ["S3_ADMIN_ACCESS_KEY", "S3_APP_ACCESS_KEY", "S3_APP_SECRET_KEY"]
    assert text.splitlines() == [
        "LOG_LEVEL=INFO",
        "S3_ADMIN_ACCESS_KEY=t1",
        "S3_ADMIN_SECRET_KEY=ya",
        "S3_APP_ACCESS_KEY=t2",
        "S3_APP_SECRET_KEY=t3",
    ]
    assert fill_blank_keys(text)[1] == []


def test_s3_config_app_solo_tiene_read_write_list_de_sus_buckets(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        s3_admin_access_key="a",
        s3_admin_secret_key="b",
        s3_app_access_key="c",
        s3_app_secret_key="d",
    )
    ids = {i["name"]: i for i in s3_config(settings)["identities"]}
    assert ids["admin"]["actions"] == ["Admin", "Read", "Write", "List", "Tagging"]
    app_actions = ids["radar-app"]["actions"]
    assert len(app_actions) == 9
    assert all(a.split(":")[0] in {"Read", "Write", "List"} for a in app_actions)
    assert {a.split(":")[1] for a in app_actions} == {"radar-bronze", "radar-silver", "radar-gold"}


def test_s3_config_sin_llaves_falla() -> None:
    with pytest.raises(ValueError):
        s3_config(Settings(_env_file=None))  # type: ignore[call-arg]


def test_ensure_buckets_es_idempotente() -> None:
    admin = FakeS3(admin=True)
    buckets = ["radar-bronze", "radar-silver", "radar-gold"]
    assert set(ensure_buckets(admin, buckets).values()) == {"creado"}
    assert set(ensure_buckets(admin, buckets).values()) == {"ya_existia"}


def test_check_policy_ok_con_minimo_privilegio() -> None:
    buckets = ["radar-bronze", "radar-silver", "radar-gold"]
    report = check_policy(FakeS3(set(buckets)), buckets)
    assert report["ok"] is True
    assert report["create_bucket"] == "AccessDenied"
    assert {r["delete_bucket"] for r in report["buckets"].values()} == {"AccessDenied"}


def test_check_policy_falla_si_la_app_es_admin() -> None:
    buckets = ["radar-bronze"]
    app = FakeS3(set(buckets), admin=True)
    report = check_policy(app, buckets, admin=app)
    assert report["ok"] is False
    assert report["buckets"]["radar-bronze"]["delete_bucket"] == "BucketNotEmpty"
    assert app.list_buckets()["Buckets"] == [{"Name": "radar-bronze"}]
