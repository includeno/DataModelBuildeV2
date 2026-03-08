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


def test_dataset_preview_missing_returns_404():
    session_id = client.post('/sessions').json()['sessionId']
    res = client.get(f"/sessions/{session_id}/datasets/missing_ds/preview?limit=1")
    assert res.status_code == 404
    assert res.json()['detail'] == 'Dataset not found'


def test_delete_missing_dataset_returns_404():
    session_id = client.post('/sessions').json()['sessionId']
    res = client.delete(f"/sessions/{session_id}/datasets/missing_ds")
    assert res.status_code == 404
    assert res.json()['detail'] == 'Dataset not found'


def test_update_dataset_schema_requires_dataset_id():
    session_id = client.post('/sessions').json()['sessionId']
    res = client.post(f"/sessions/{session_id}/datasets/update", json={"fieldTypes": {"a": {"type": "string"}}})
    assert res.status_code == 400
    assert res.json()['detail'] == 'datasetId is required'


def test_update_dataset_schema_requires_field_types():
    session_id = client.post('/sessions').json()['sessionId']
    res = client.post(f"/sessions/{session_id}/datasets/update", json={"datasetId": "people"})
    assert res.status_code == 400
    assert res.json()['detail'] == 'fieldTypes is required'
