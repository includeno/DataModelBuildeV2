
import pytest
from playwright.sync_api import Page, expect
import os

# CONFIGURATION
# Run the frontend (npm run dev) and backend (npm run backend) before running tests.
APP_URL = "http://localhost:1420"
BACKEND_URL_UI = "http://localhost:8000"

def test_homepage_load(page: Page):
    """Verify the application loads and shows key components."""
    page.goto(APP_URL)
    expect(page).to_have_title("Hierarchical Data Engine")
    expect(page.locator("h1")).to_contain_text("DataFlow Engine")
    expect(page.get_by_role("button", name="Session")).to_be_visible()

def test_switch_to_mock_and_query(page: Page):
    """
    Test switching to Mock Server mode and running a predefined query.
    This ensures the frontend logic works independently of the Python backend.
    """
    page.goto(APP_URL)
    
    # 1. Open Settings
    page.get_by_title("Global Settings (Connection & Appearance)").click()
    
    # 2. Select Mock Server
    page.get_by_text("Mock Server").click()
    page.get_by_role("button", name="Done").click()
    
    # 3. Create Session (Mock)
    page.locator("button:has-text('Session')").click()
    page.get_by_text("Create New Session").click()
    
    # 4. Navigate to SQL View
    page.locator("button:has-text('SQL Studio')").click()
    
    # 5. Run Mock Query
    # Mock server handles "SELECT * FROM mock_employees"
    editor = page.locator("textarea")
    editor.fill("SELECT * FROM mock_employees")
    page.get_by_role("button", name="Run Query").click()
    
    # 6. Verify Results
    # "Alice" is in the mock data generator
    expect(page.get_by_text("Alice")).to_be_visible()
    expect(page.get_by_text("Engineering")).to_be_visible()

def test_e2e_backend_workflow(page: Page):
    """
    Full E2E: Connect to Python Backend -> Upload CSV -> Filter -> View Result.
    Note: Requires 'npm run backend' running at localhost:8000.
    """
    page.goto(APP_URL)
    
    # 1. Connect to Python Backend
    page.get_by_title("Global Settings (Connection & Appearance)").click()
    
    # Try finding the backend option, add if missing
    try:
        backend_opt = page.get_by_text(BACKEND_URL_UI)
        if backend_opt.count() > 0 and backend_opt.is_visible():
            backend_opt.click()
        else:
            page.fill("input[placeholder='http://192.168.1.10:8000']", BACKEND_URL_UI)
            page.locator("button:has(svg.lucide-plus)").click()
            page.get_by_text(BACKEND_URL_UI).click()
    except:
        # Fallback if UI state is complex
        pass
        
    page.get_by_role("button", name="Done").click()
    
    # 2. Create Session
    page.locator("button:has-text('Session')").click()
    page.get_by_text("Create New Session").click()
    
    # 3. Upload Data
    # Create a dummy CSV file on the fly
    with open("temp_test_data.csv", "w") as f:
        f.write("id,category,score\n1,A,50\n2,B,80\n3,A,90")
        
    page.locator("button[title='Import Dataset']").click()
    
    # Handle file chooser
    with page.expect_file_chooser() as fc_info:
        page.locator("text=Click to upload").click()
    file_chooser = fc_info.value
    file_chooser.set_files("temp_test_data.csv")
    
    # Confirm Upload
    page.wait_for_selector("input[value='temp_test_data']")
    page.get_by_role("button", name="Import Dataset").click()
    
    # 4. Build Pipeline
    # Select Root (should auto-select 'temp_test_data' source if it's the first one, or user selects it)
    page.get_by_text("Root").click()
    
    # Add Child -> Filter
    page.locator("button[title='Add Child']").click()
    page.get_by_text("New Operation").click()
    
    # Configure Filter: score > 60
    # Wait for panel to load
    page.wait_for_selector("text=Operation #")
    
    # Add Rule
    page.get_by_text("Add Rule").click()
    
    # Set Field (using Selectors)
    # Note: Selectors might be tricky with custom dropdowns, using direct options if standard select
    # Or typing if searchable. Assuming standard select implementation in CommandEditor.
    page.locator("select").nth(1).select_option("score") # Field
    page.locator("select").nth(2).select_option(">")     # Operator
    page.locator("input[placeholder='Value']").fill("60")
    
    # 5. Run
    page.get_by_role("button", name="Run").click()
    
    # 6. Verify Result Panel
    # Rows with score 80 and 90 should show. 50 should be gone.
    # Total count should be 2.
    expect(page.get_by_text("2 Rows")).to_be_visible()
    expect(page.get_by_text("80")).to_be_visible()
    expect(page.get_by_text("90")).to_be_visible()
    
    # Cleanup
    if os.path.exists("temp_test_data.csv"):
        os.remove("temp_test_data.csv")

if __name__ == "__main__":
    # Instructions to run manually
    print("Run with: pytest scripts/test_e2e_ui.py")
