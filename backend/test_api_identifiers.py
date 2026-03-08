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


def test_dataset_with_reserved_and_hyphen_names():
    session_id = client.post("/sessions").json()["sessionId"]
    df = pd.DataFrame({
        "order": [1, 2],
        "line-item": [10, 20]
    })

    with pytest.raises(ValueError):
        storage.add_dataset(session_id, "order", df)

    table_name = storage.add_dataset(session_id, "order-items", df)
    assert table_name == "order-items"

    with pytest.raises(ValueError):
        storage.get_full_dataset(session_id, "order")

    preview = storage.get_dataset_preview(session_id, "order-items", limit=1)
    assert preview is not None
    assert preview.shape[0] == 1

    full_df = storage.get_full_dataset(session_id, "order-items")
    assert full_df is not None
    assert list(full_df.columns) == ["order", "line-item"]

    # SQL execution should work with quoted identifiers for non-reserved hyphenated table name
    result = storage.execute_sql(session_id, 'SELECT "order", "line-item" FROM "order-items"')
    assert result.shape[0] == 2
