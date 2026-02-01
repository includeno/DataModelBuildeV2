
import os
import time
import json
import re
from playwright.sync_api import sync_playwright

FILES_TO_UPLOAD = [
    "ecommerce_orders.csv",
    "hr_employees.csv",
    "iot_logs.csv",
    "financial_ledger.csv",
    "student_scores.csv",
    "inventory_items.csv"
]

TEST_DATA_DIR = os.path.abspath("test_data")
# Ensure backend directory exists for config
if not os.path.exists("backend"):
    os.makedirs("backend")
SESSION_CONFIG_PATH = os.path.abspath("backend/session_config.json")

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("Navigating to frontend...")
        try:
            page.goto("http://localhost:1420", timeout=30000)
        except Exception as e:
            print(f"Failed to load frontend: {e}")
            page.goto("http://localhost:1420")
        
        # 1. Switch to Real Backend
        print("Switching to Real Backend...")
        try:
            page.get_by_title("Global Settings (Connection & Appearance)").click()
        except:
             print("Could not find settings button")
             print(page.content())
             exit(1)
        
        try:
            localhost_opt = page.get_by_text("http://localhost:8000")
            if localhost_opt.count() > 0:
                localhost_opt.first.click()
            else:
                page.fill("input[placeholder='http://192.168.1.10:8000']", "http://localhost:8000")
                page.locator("button:has(svg.lucide-plus)").click()
                page.get_by_text("http://localhost:8000").click()
        except Exception as e:
            print(f"Error selecting backend: {e}")
            
        page.get_by_role("button", name="Done").click()
        
        # 2. Create New Session
        print("Creating new session...")
        time.sleep(1)
        
        session_btn = page.locator("button:has-text('Session')")
        session_btn.click()
        page.get_by_text("Create New Session").click()
        time.sleep(2)
        
        # 3. Upload files
        print("Uploading files...")
        
        import_btn = page.locator("button[title='Import Dataset']")
        
        captured_session_id = None
        
        for filename in FILES_TO_UPLOAD:
            filepath = os.path.join(TEST_DATA_DIR, filename)
            if not os.path.exists(filepath):
                print(f"File not found: {filepath}")
                continue
                
            print(f"Uploading {filename}...")
            
            import_btn.click()
            page.wait_for_selector("text=Import Data Source")
            
            # Use explicit file chooser
            try:
                with page.expect_file_chooser() as fc_info:
                    # Click the dropzone (wrapper div)
                    page.locator("text=Click to upload").click()
                file_chooser = fc_info.value
                file_chooser.set_files(filepath)
            except Exception as e:
                print(f"Error selecting file via chooser: {e}")
                exit(1)
            
            # Wait for filename to appear (indicates selection success)
            try:
                # Wait for the view to switch to the file details view
                # The "Click to upload" text should disappear
                page.wait_for_selector("text=Click to upload", state="hidden")
                
                # Verify dataset name input appears
                page.locator("input[placeholder='Enter a name for this dataset']").wait_for()
            except Exception as e:
                print(f"Timeout waiting for UI update after upload: {e}")
                print(page.content())
                exit(1)

            with page.expect_response(lambda response: "/upload" in response.url and response.status == 200) as response_info:
                # Use filter(has_text=...) to avoid matching the sidebar button which has title="Import Dataset"
                page.locator("button").filter(has_text="Import Dataset").click()
            
            page.wait_for_selector("text=Import Data Source", state="hidden")
            print(f"Uploaded {filename}")
            
        session_btn_text = page.locator("button:has-text('Session')").text_content()
        print(f"Session Text: {session_btn_text}")
        match = re.search(r'(sess_[a-f0-9]+)', session_btn_text)
        if match:
            captured_session_id = match.group(1)
            print(f"Captured Session ID: {captured_session_id}")
            with open(SESSION_CONFIG_PATH, 'w') as f:
                json.dump({"session_id": captured_session_id}, f)
        else:
            print("Failed to capture Session ID from UI")
            exit(1)
            
        browser.close()

if __name__ == "__main__":
    run()
