
import pytest
from playwright.sync_api import Page, expect
import httpx
import subprocess
import time
import os
import signal
import sys
import shutil

# Constants
BACKEND_PORT = 8000
FRONTEND_PORT = 1420
BACKEND_URL = f"http://localhost:{BACKEND_PORT}"
FRONTEND_URL = f"http://localhost:{FRONTEND_PORT}"

def wait_for_service(url, name, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            httpx.get(url)
            print(f"{name} is ready at {url}.")
            return
        except Exception as e:
            # print(f"Waiting for {name}... {e}")
            time.sleep(1)
    raise RuntimeError(f"{name} failed to start within {timeout} seconds at {url}.")

@pytest.fixture(scope="module")
def services():
    print("\nStarting services...")

    # Files for logs
    backend_log = open("backend_server.log", "w")
    frontend_log = open("frontend_server.log", "w")

    # Start Backend
    backend_env = os.environ.copy()
    backend_process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--port", str(BACKEND_PORT)],
        cwd="backend",
        env=backend_env,
        stdout=backend_log,
        stderr=subprocess.STDOUT
    )

    # Start Frontend
    frontend_process = subprocess.Popen(
        ["npm", "run", "dev", "--", "--port", str(FRONTEND_PORT), "--host"],
        cwd=".",
        stdout=frontend_log,
        stderr=subprocess.STDOUT
    )

    try:
        # Wait for services to be ready
        wait_for_service(f"{BACKEND_URL}/sessions", "Backend")

        try:
            wait_for_service(FRONTEND_URL, "Frontend")
        except RuntimeError as e:
            print(f"Frontend failed to start: {e}")
            if frontend_process.poll() is not None:
                print(f"Frontend process exited with code {frontend_process.returncode}")

            # Print logs
            frontend_log.flush()
            with open("frontend_server.log", "r") as f:
                print("Frontend Logs:\n", f.read())
            raise e

        yield
    finally:
        print("\nStopping services...")
        backend_process.terminate()
        frontend_process.terminate()
        try:
            backend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            backend_process.kill()
        try:
            frontend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            frontend_process.kill()

        backend_log.close()
        frontend_log.close()

def test_ui_api_flow(page: Page, services):
    print("Navigating to Frontend...")
    # 1. Open Web App
    page.goto(FRONTEND_URL)
    expect(page).to_have_title("Hierarchical Data Engine")

    # 2. Connect to Real Backend
    print("Configuring Backend Connection...")
    page.get_by_title("Global Settings (Connection & Appearance)").click()

    try:
        if page.get_by_text(BACKEND_URL).count() > 0:
             page.get_by_text(BACKEND_URL).click()
        else:
             page.fill("input[placeholder='http://192.168.1.10:8000']", BACKEND_URL)
             page.locator("button:has(svg.lucide-plus)").click()
             page.get_by_text(BACKEND_URL).click()
    except Exception as e:
        print(f"Error selecting backend: {e}")

    page.get_by_role("button", name="Done").click()

    # 3. Create Session
    print("Creating Session...")
    page.locator("button:has-text('Session')").click()
    page.get_by_text("Create New Session").click()

    time.sleep(1)

    # 4. Upload CSV
    print("Uploading CSV...")
    csv_path = os.path.abspath("test_data/ecommerce_orders.csv")
    if not os.path.exists(csv_path):
        pytest.fail(f"CSV file not found at {csv_path}")

    page.locator("button[title='Import Dataset']").click()

    with page.expect_file_chooser() as fc_info:
        page.locator("text=Click to upload").click()
    file_chooser = fc_info.value
    file_chooser.set_files(csv_path)

    page.wait_for_selector("input[value='ecommerce_orders']")
    page.locator("div.fixed button").filter(has_text="Import Dataset").click()

    print("Waiting for dataset to appear...")
    page.wait_for_selector("text=ecommerce_orders", state="visible", timeout=10000)

    # 5. Extract Session ID via API
    print("Verifying via API...")
    response = httpx.get(f"{BACKEND_URL}/sessions")
    assert response.status_code == 200
    sessions = response.json()
    assert len(sessions) > 0

    session_id = sessions[0]['sessionId']
    print(f"Session ID: {session_id}")

    # 6. Verify Dataset via API
    response = httpx.get(f"{BACKEND_URL}/sessions/{session_id}/datasets")
    assert response.status_code == 200
    datasets = response.json()
    print(f"Datasets: {datasets}")

    dataset_names = [d['name'] for d in datasets]
    assert "ecommerce_orders" in dataset_names

    # 7. Execute Query via API
    print("Executing SQL Query via API...")
    sql_payload = {
        "sessionId": session_id,
        "query": "SELECT * FROM ecommerce_orders LIMIT 5",
        "page": 1,
        "pageSize": 10
    }

    response = httpx.post(f"{BACKEND_URL}/query", json=sql_payload)
    if response.status_code != 200:
        print(f"Query failed: {response.text}")
    assert response.status_code == 200
    data = response.json()

    assert "rows" in data
    assert len(data["rows"]) == 5
    print("Rows returned:", len(data["rows"]))

    first_row = data["rows"][0]
    assert "order_id" in first_row

    print("Integration Test Passed Successfully!")

if __name__ == "__main__":
    pass
