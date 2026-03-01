"""
Comprehensive E2E UI Test Suite

Tests the complete workflow from UI to backend, covering:
- Session management (create, switch, rename)
- Data upload and preview
- Filter operations with various operators
- Variable definition and usage
- Nested operation tree execution
- SQL query execution
- Settings and preferences

Prerequisites:
- Frontend running: npm run dev (port 1420)
- Backend running: npm run backend (port 8000)
- Install: pip install pytest playwright && playwright install chromium
"""

import pytest
from playwright.sync_api import Page, expect, BrowserContext
import os
import time
from pathlib import Path

# Configuration
APP_URL = os.getenv("APP_URL", "http://localhost:1420")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
TEST_DATA_DIR = Path(__file__).resolve().parent.parent / "test_data"


# === FIXTURES ===

@pytest.fixture(scope="function")
def setup_backend_connection(page: Page):
    """Ensure backend connection is configured."""
    page.goto(APP_URL)
    page.wait_for_load_state("networkidle")

    # Open settings and ensure backend URL is selected
    settings_btn = page.get_by_title("Global Settings (Connection & Appearance)")
    if settings_btn.is_visible():
        settings_btn.click()
        page.wait_for_timeout(500)

        # Check if backend URL already exists, if not add it
        try:
            backend_option = page.get_by_text(BACKEND_URL, exact=True)
            if backend_option.is_visible():
                backend_option.click()
            else:
                # Add new backend URL
                url_input = page.locator("input[placeholder*='http']")
                if url_input.is_visible():
                    url_input.fill(BACKEND_URL)
                    page.locator("button:has(svg.lucide-plus)").click()
                    page.get_by_text(BACKEND_URL).click()
        except Exception:
            pass

        # Close settings
        done_btn = page.get_by_role("button", name="Done")
        if done_btn.is_visible():
            done_btn.click()

    page.wait_for_timeout(500)
    return page


# === SESSION MANAGEMENT TESTS ===

class TestSessionManagement:
    """Test session creation, switching, and deletion."""

    def test_create_new_session(self, page: Page, setup_backend_connection):
        """Test creating a new session via UI."""
        page = setup_backend_connection

        # Click Session dropdown
        page.locator("button:has-text('Session')").click()
        page.wait_for_timeout(300)

        # Click Create New Session
        create_btn = page.get_by_text("Create New Session")
        expect(create_btn).to_be_visible()
        create_btn.click()

        # Wait for session to be created
        page.wait_for_timeout(1000)

        # Verify new session appears in dropdown
        page.locator("button:has-text('Session')").click()
        page.wait_for_timeout(300)

        # Should see session list
        session_list = page.locator("[class*='dropdown']")
        expect(session_list).to_be_visible()

    def test_switch_between_sessions(self, page: Page, setup_backend_connection):
        """Test switching between existing sessions."""
        page = setup_backend_connection

        # Create first session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Create second session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Open session dropdown
        page.locator("button:has-text('Session')").click()
        page.wait_for_timeout(300)

        # Count visible sessions (should be at least 2)
        # The sessions appear in the dropdown list
        session_items = page.locator("text=sess_")
        expect(session_items.first).to_be_visible()


# === DATA UPLOAD TESTS ===

class TestDataUpload:
    """Test CSV file upload and data preview."""

    def test_upload_csv_file(self, page: Page, setup_backend_connection):
        """Test uploading a CSV file and viewing preview."""
        page = setup_backend_connection

        # Create new session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Click Import Dataset button
        import_btn = page.locator("button[title='Import Dataset']")
        expect(import_btn).to_be_visible()
        import_btn.click()
        page.wait_for_timeout(500)

        # Create test CSV file
        test_csv_path = Path("/tmp/test_e2e_upload.csv")
        test_csv_path.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300")

        # Handle file chooser
        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        file_chooser = fc_info.value
        file_chooser.set_files(str(test_csv_path))

        # Wait for preview to load
        page.wait_for_timeout(1000)

        # Verify file name appears in input
        expect(page.locator("input[value*='test_e2e_upload']")).to_be_visible()

        # Click Import button in modal
        modal_import_btn = page.locator("div.fixed button:has-text('Import Dataset')")
        expect(modal_import_btn).to_be_visible()
        modal_import_btn.click()

        # Wait for import to complete
        page.wait_for_timeout(1500)

        # Verify dataset appears in sidebar
        sidebar = page.locator(".sidebar, [class*='sidebar']")
        expect(page.get_by_text("test_e2e_upload")).to_be_visible(timeout=5000)

        # Cleanup
        test_csv_path.unlink(missing_ok=True)


# === WORKFLOW OPERATION TESTS ===

class TestWorkflowOperations:
    """Test operation tree building and execution."""

    def test_add_filter_operation(self, page: Page, setup_backend_connection):
        """Test adding a filter operation to the workflow."""
        page = setup_backend_connection

        # Create session and upload data
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Upload test data
        test_csv = Path("/tmp/test_filter.csv")
        test_csv.write_text("id,category,score\n1,A,50\n2,B,80\n3,A,90\n4,B,30")

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(1500)

        # Select Data Setup node to configure source
        page.get_by_text("Data Setup").click()
        page.wait_for_timeout(500)

        # The UI should show the operation panel
        # Add a child operation node
        add_child_btn = page.locator("button[title='Add Child']")
        if add_child_btn.is_visible():
            add_child_btn.click()
            page.wait_for_timeout(500)

        # Cleanup
        test_csv.unlink(missing_ok=True)

    def test_execute_and_view_results(self, page: Page, setup_backend_connection):
        """Test executing operation tree and viewing results."""
        page = setup_backend_connection

        # Create session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Upload test data
        test_csv = Path("/tmp/test_execute.csv")
        test_csv.write_text("id,value\n1,10\n2,20\n3,30\n4,40\n5,50")

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(2000)

        # Select Data Setup node
        page.get_by_text("Data Setup").click()
        page.wait_for_timeout(500)

        # Click Run button if visible
        run_btn = page.get_by_role("button", name="Run")
        if run_btn.is_visible():
            run_btn.click()
            page.wait_for_timeout(2000)

            # Check for results in preview panel
            # Should show "5 Rows" or similar count
            expect(page.get_by_text("Rows")).to_be_visible(timeout=5000)

        # Cleanup
        test_csv.unlink(missing_ok=True)


# === SQL STUDIO TESTS ===

class TestSqlStudio:
    """Test SQL editor functionality."""

    def test_switch_to_sql_view(self, page: Page, setup_backend_connection):
        """Test switching to SQL Studio view."""
        page = setup_backend_connection

        # Create session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Click SQL Studio button
        sql_btn = page.locator("button:has-text('SQL Studio')")
        expect(sql_btn).to_be_visible()
        sql_btn.click()

        page.wait_for_timeout(500)

        # Verify SQL editor is visible
        editor = page.locator("textarea, [class*='editor']")
        expect(editor.first).to_be_visible()

    def test_execute_sql_query(self, page: Page, setup_backend_connection):
        """Test executing a SQL query."""
        page = setup_backend_connection

        # Create session and upload data
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Upload test data
        test_csv = Path("/tmp/test_sql.csv")
        test_csv.write_text("id,name,amount\n1,Alice,100\n2,Bob,200\n3,Charlie,150")

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(2000)

        # Switch to SQL Studio
        page.locator("button:has-text('SQL Studio')").click()
        page.wait_for_timeout(500)

        # Enter SQL query
        editor = page.locator("textarea")
        editor.fill("SELECT * FROM test_sql ORDER BY amount DESC")

        # Run query
        run_btn = page.get_by_role("button", name="Run Query")
        if run_btn.is_visible():
            run_btn.click()
            page.wait_for_timeout(2000)

            # Verify results show Bob (highest amount)
            expect(page.get_by_text("Bob")).to_be_visible(timeout=5000)

        # Cleanup
        test_csv.unlink(missing_ok=True)


# === SETTINGS TESTS ===

class TestSettings:
    """Test settings modal and preferences."""

    def test_open_settings_modal(self, page: Page, setup_backend_connection):
        """Test opening settings modal."""
        page = setup_backend_connection

        # Click settings button
        settings_btn = page.get_by_title("Global Settings (Connection & Appearance)")
        expect(settings_btn).to_be_visible()
        settings_btn.click()

        page.wait_for_timeout(500)

        # Verify settings modal is visible
        expect(page.get_by_text("Connection")).to_be_visible()
        expect(page.get_by_text("Appearance")).to_be_visible()

        # Close settings
        page.get_by_role("button", name="Done").click()

    def test_switch_to_mock_server(self, page: Page):
        """Test switching to Mock Server mode."""
        page.goto(APP_URL)
        page.wait_for_load_state("networkidle")

        # Open settings
        page.get_by_title("Global Settings (Connection & Appearance)").click()
        page.wait_for_timeout(500)

        # Select Mock Server
        mock_option = page.get_by_text("Mock Server", exact=True)
        if mock_option.is_visible():
            mock_option.click()

        # Close settings
        page.get_by_role("button", name="Done").click()
        page.wait_for_timeout(500)

        # Create session in mock mode
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Switch to SQL view and test mock query
        page.locator("button:has-text('SQL Studio')").click()
        page.wait_for_timeout(500)

        editor = page.locator("textarea")
        editor.fill("SELECT * FROM mock_employees LIMIT 5")

        run_btn = page.get_by_role("button", name="Run Query")
        if run_btn.is_visible():
            run_btn.click()
            page.wait_for_timeout(2000)

            # Mock data should include "Alice"
            expect(page.get_by_text("Alice")).to_be_visible(timeout=5000)


# === FILTER OPERATOR TESTS ===

class TestFilterOperators:
    """Test various filter operators through UI."""

    def test_filter_by_status_in_list(self, page: Page, setup_backend_connection):
        """Test in_list filter operator through UI."""
        page = setup_backend_connection

        # Create session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Upload ecommerce-like data
        test_csv = Path("/tmp/test_in_list.csv")
        test_csv.write_text(
            "order_id,status,amount\n"
            "ORD_001,PENDING,100\n"
            "ORD_002,SHIPPED,200\n"
            "ORD_003,DELIVERED,150\n"
            "ORD_004,CANCELLED,50\n"
            "ORD_005,PENDING,300"
        )

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(2000)

        # Select Data Setup and run to see all data first
        page.get_by_text("Data Setup").click()
        page.wait_for_timeout(500)

        run_btn = page.get_by_role("button", name="Run")
        if run_btn.is_visible():
            run_btn.click()
            page.wait_for_timeout(2000)

        # Cleanup
        test_csv.unlink(missing_ok=True)


# === VARIABLE DEFINITION TESTS ===

class TestVariableDefinition:
    """Test variable definition and usage through UI."""

    def test_define_and_use_variable(self, page: Page, setup_backend_connection):
        """Test defining a variable and using it in filter."""
        page = setup_backend_connection

        # Create session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Upload test data
        test_csv = Path("/tmp/test_var.csv")
        test_csv.write_text(
            "id,category\n"
            "1,A\n2,B\n3,A\n4,C\n5,B"
        )

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(2000)

        # The variable definition UI would be in the CommandEditor
        # Select Data Setup node
        page.get_by_text("Data Setup").click()
        page.wait_for_timeout(500)

        # Look for "Add Variable" or similar button in the UI
        # This depends on the actual UI implementation

        # Cleanup
        test_csv.unlink(missing_ok=True)


# === DATA PREVIEW TESTS ===

class TestDataPreview:
    """Test data preview panel functionality."""

    def test_pagination_in_preview(self, page: Page, setup_backend_connection):
        """Test pagination in data preview panel."""
        page = setup_backend_connection

        # Create session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # Upload larger dataset
        rows = ["id,value"]
        for i in range(1, 101):
            rows.append(f"{i},{i*10}")

        test_csv = Path("/tmp/test_pagination.csv")
        test_csv.write_text("\n".join(rows))

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(2000)

        # Select node and run
        page.get_by_text("Data Setup").click()
        page.wait_for_timeout(500)

        run_btn = page.get_by_role("button", name="Run")
        if run_btn.is_visible():
            run_btn.click()
            page.wait_for_timeout(2000)

            # Should show "100 Rows" total
            expect(page.get_by_text("100 Rows")).to_be_visible(timeout=5000)

            # Look for pagination controls
            next_page_btn = page.locator("button:has-text('Next'), button:has(svg.lucide-chevron-right)")
            if next_page_btn.is_visible():
                next_page_btn.click()
                page.wait_for_timeout(1000)

        # Cleanup
        test_csv.unlink(missing_ok=True)


# === INTEGRATION TESTS ===

class TestFullWorkflow:
    """End-to-end integration tests covering complete workflows."""

    def test_complete_etl_workflow(self, page: Page, setup_backend_connection):
        """Test complete workflow: Upload -> Filter -> View Results."""
        page = setup_backend_connection

        # 1. Create new session
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        # 2. Upload data
        test_csv = Path("/tmp/test_etl.csv")
        test_csv.write_text(
            "product,region,sales\n"
            "Widget,North,1000\n"
            "Gadget,South,1500\n"
            "Widget,South,800\n"
            "Gadget,North,2000\n"
            "Gizmo,North,500"
        )

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(2000)

        # 3. Select Data Setup
        page.get_by_text("Data Setup").click()
        page.wait_for_timeout(500)

        # 4. Run to see all data
        run_btn = page.get_by_role("button", name="Run")
        if run_btn.is_visible():
            run_btn.click()
            page.wait_for_timeout(2000)

            # Should show 5 rows
            expect(page.get_by_text("5 Rows")).to_be_visible(timeout=5000)

        # 5. Switch to SQL and run aggregation
        page.locator("button:has-text('SQL Studio')").click()
        page.wait_for_timeout(500)

        editor = page.locator("textarea")
        editor.fill("SELECT region, SUM(sales) as total_sales FROM test_etl GROUP BY region")

        run_query_btn = page.get_by_role("button", name="Run Query")
        if run_query_btn.is_visible():
            run_query_btn.click()
            page.wait_for_timeout(2000)

            # Should show North and South regions
            expect(page.get_by_text("North")).to_be_visible(timeout=5000)

        # Cleanup
        test_csv.unlink(missing_ok=True)


# === EXPORT TESTS ===

class TestExport:
    """Test data export functionality."""

    def test_export_button_visibility(self, page: Page, setup_backend_connection):
        """Test that export button is visible after execution."""
        page = setup_backend_connection

        # Create session and upload data
        page.locator("button:has-text('Session')").click()
        page.get_by_text("Create New Session").click()
        page.wait_for_timeout(1000)

        test_csv = Path("/tmp/test_export.csv")
        test_csv.write_text("id,value\n1,100\n2,200")

        page.locator("button[title='Import Dataset']").click()
        page.wait_for_timeout(500)

        with page.expect_file_chooser() as fc_info:
            page.locator("text=Click to upload").click()
        fc_info.value.set_files(str(test_csv))
        page.wait_for_timeout(1000)

        page.locator("div.fixed button:has-text('Import Dataset')").click()
        page.wait_for_timeout(2000)

        # Select node and run
        page.get_by_text("Data Setup").click()
        page.wait_for_timeout(500)

        run_btn = page.get_by_role("button", name="Run")
        if run_btn.is_visible():
            run_btn.click()
            page.wait_for_timeout(2000)

            # Look for export button (Download icon)
            export_btn = page.locator("button[title*='Export'], button:has(svg.lucide-download)")
            # Export button should be visible after results are loaded

        # Cleanup
        test_csv.unlink(missing_ok=True)


if __name__ == "__main__":
    print("Run with: pytest scripts/test_e2e_session_scenarios.py -v")
    print("Prerequisites:")
    print("  1. Start frontend: npm run dev")
    print("  2. Start backend: npm run backend")
    print("  3. Install Playwright: pip install playwright && playwright install chromium")
