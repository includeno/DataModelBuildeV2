import json
import os
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

import main as main_module
from storage import storage

client = TestClient(main_module.app)


@pytest.fixture(autouse=True)
def clean_env():
    storage.clear()
    yield
    storage.clear()


def _parquet_bytes(rows):
    df = pd.DataFrame(rows)
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        tmp_path = tmp.name
    con = duckdb.connect()
    try:
        con.register('df', df)
        con.execute(f"COPY df TO '{tmp_path}' (FORMAT PARQUET)")
    finally:
        con.close()
    data = Path(tmp_path).read_bytes()
    os.remove(tmp_path)
    return data


def test_parquet_same_name_replaces_dataset_file():
    session_id = client.post('/sessions').json()['sessionId']

    data1 = _parquet_bytes([{"id": 1, "name": "A"}])
    res1 = client.post(
        '/upload',
        files={'file': ('data.parquet', data1, 'application/octet-stream')},
        data={'sessionId': session_id, 'name': 'same_dataset'}
    )
    assert res1.status_code == 200
    payload1 = res1.json()
    assert payload1['name'] == 'same_dataset'
    assert payload1['totalCount'] == 1

    datasets_path = Path(storage._get_session_path(session_id)) / 'datasets.json'
    datasets1 = Path(datasets_path).read_text(encoding='utf-8')
    data_list1 = json.loads(datasets1)
    assert len(data_list1) == 1
    entry1 = data_list1[0]
    old_file_path = storage._resolve_dataset_file_path(session_id, entry1)
    assert old_file_path and os.path.exists(old_file_path)

    data2 = _parquet_bytes([
        {"id": 2, "name": "B"},
        {"id": 3, "name": "C"}
    ])
    res2 = client.post(
        '/upload',
        files={'file': ('data.parquet', data2, 'application/octet-stream')},
        data={'sessionId': session_id, 'name': 'same_dataset'}
    )
    assert res2.status_code == 200
    payload2 = res2.json()
    assert payload2['name'] == 'same_dataset'
    assert payload2['totalCount'] == 2

    datasets2 = Path(datasets_path).read_text(encoding='utf-8')
    data_list2 = json.loads(datasets2)
    assert len(data_list2) == 1
    entry2 = data_list2[0]
    new_file_path = storage._resolve_dataset_file_path(session_id, entry2)

    assert entry2['id'] == 'same_dataset'
    assert entry2['totalCount'] == 2
    assert new_file_path and os.path.exists(new_file_path)
    assert new_file_path != old_file_path
    assert not os.path.exists(old_file_path)
