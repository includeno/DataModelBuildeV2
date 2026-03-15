import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright


APP_URL = "http://localhost:1420"
BACKEND_URL_UI = "http://localhost:8000"
TARGET_CASES = 50
MAX_SQL_CANDIDATES = 90

OUTPUT_PATH = Path("backend/test_fixtures/sql_export_ui_generated_cases.json")
ORDERS_CSV = Path(__file__).resolve().parents[1] / "test_data" / "orders.csv"


def _build_sql_candidates() -> List[str]:
    return [f"SELECT * FROM orders WHERE amount > {i}" for i in range(1, MAX_SQL_CANDIDATES + 1)]


def _select_backend(page: Page) -> None:
    page.get_by_title("Global Settings (Connection & Appearance)").click()
    backend_opt = page.get_by_text(BACKEND_URL_UI)
    if backend_opt.count() > 0:
        backend_opt.first.click()
    else:
        page.fill("input[placeholder='http://192.168.1.10:8000']", BACKEND_URL_UI)
        page.locator("button:has(svg.lucide-plus)").click()
        page.get_by_text(BACKEND_URL_UI).first.click()
    page.get_by_role("button", name="Done").click()


def _create_session(page: Page) -> None:
    page.locator("button:has-text('Session')").click()
    page.get_by_text("Create New Session").click()
    page.wait_for_timeout(500)


def _upload_orders_dataset(page: Page) -> None:
    page.locator("button[title='Import Dataset']").click()
    with page.expect_file_chooser() as fc_info:
        page.get_by_text("Click to upload").click()
    fc_info.value.set_files(str(ORDERS_CSV))
    page.locator("input[placeholder='Enter a name for this dataset']").fill("orders")
    page.locator("div.fixed button").filter(has_text="Import Dataset").click()
    page.get_by_text("orders", exact=True).first.wait_for(state="visible")


def _configure_setup_source(page: Page, dataset_name: str = "orders") -> None:
    page.get_by_title("Data Setup", exact=True).click()
    page.get_by_role("button", name="Add Data Source").click()
    page.get_by_text("-- Select Dataset --").last.click()
    try:
        page.get_by_role("option", name=re.compile(rf"^{re.escape(dataset_name)}\\b")).first.click(timeout=5000)
    except PlaywrightTimeoutError:
        page.get_by_role("option").first.click(timeout=5000)
    page.wait_for_timeout(200)


def _ensure_process_operation(page: Page) -> None:
    page.get_by_role("button", name="Add Child").first.click()
    page.get_by_role("button", name="Build from SQL").first.wait_for(state="visible")
    op_name = page.get_by_placeholder("Operation Name")
    if op_name.count() > 0:
        op_name.first.fill("UI SQL Export Cases")


def _open_sql_builder(page: Page) -> None:
    page.get_by_role("button", name="Build from SQL").first.click()
    page.locator("[data-testid='sql-builder-modal']").wait_for(state="visible")


def _apply_sql_in_builder(page: Page, sql_text: str) -> bool:
    page.locator("[data-testid='sql-builder-input']").fill(sql_text)
    page.get_by_role("button", name="Parse").click()
    apply_btn = page.get_by_role("button", name="Apply")
    apply_btn.wait_for(state="visible")
    if apply_btn.is_disabled():
        page.get_by_role("button", name="Cancel").click()
        return False
    apply_btn.click()
    page.locator("[data-testid='sql-builder-modal']").wait_for(state="hidden")
    return True


def _find_command_in_tree(node: Dict[str, Any], command_id: str) -> Optional[Dict[str, Any]]:
    for cmd in node.get("commands", []):
        if cmd.get("id") == command_id:
            return cmd
    for child in node.get("children", []) or []:
        found = _find_command_in_tree(child, command_id)
        if found:
            return found
    return None


def _capture_last_command(page: Page) -> Optional[Dict[str, Any]]:
    with page.expect_response(
        lambda resp: "/generate_sql" in resp.url and resp.request.method == "POST",
        timeout=30000,
    ) as resp_info:
        page.get_by_role("button", name="Generate SQL").last.click()
    response = resp_info.value
    if response.status != 200:
        return None

    payload = response.request.post_data_json
    response_json = response.json()
    target_cmd_id = payload.get("targetCommandId")
    tree = payload.get("tree") or {}
    cmd = _find_command_in_tree(tree, target_cmd_id)
    if not cmd:
        return None

    captured = {
        "payload": payload,
        "targetCommand": cmd,
        "generatedSql": response_json.get("sql", ""),
    }
    close_btn = page.get_by_role("button", name="Close")
    try:
        close_btn.first.wait_for(state="visible", timeout=1500)
        close_btn.first.click()
    except PlaywrightTimeoutError:
        pass
    return captured


def main() -> None:
    sql_candidates = _build_sql_candidates()
    unique_keys: Set[str] = set()
    collected: List[Dict[str, Any]] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(APP_URL, wait_until="domcontentloaded", timeout=60000)

        _select_backend(page)
        _create_session(page)
        _upload_orders_dataset(page)
        _configure_setup_source(page, "orders")
        _ensure_process_operation(page)

        for idx, sql in enumerate(sql_candidates, start=1):
            _open_sql_builder(page)
            applied = _apply_sql_in_builder(page, sql)
            if not applied:
                print(f"[skip] apply disabled for SQL: {sql}")
                continue
            captured = _capture_last_command(page)
            if not captured:
                continue

            cmd = captured["targetCommand"]
            normalized = {"type": cmd.get("type"), "config": cmd.get("config", {})}
            key = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
            if key in unique_keys:
                continue
            unique_keys.add(key)

            case_id = f"ui_filter_amount_gt_{idx}"
            threshold = idx
            collected.append(
                {
                    "caseId": case_id,
                    "sourceSql": sql,
                    "command": cmd,
                    "expectedTokens": [
                        "SELECT * FROM orders",
                        f"amount > {threshold}",
                    ],
                    "uiGeneratedSql": captured["generatedSql"],
                }
            )
            print(f"[collect] {case_id} -> {cmd.get('type')} ({len(collected)}/{TARGET_CASES})")
            if len(collected) >= TARGET_CASES:
                break

        browser.close()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "generatedBy": "scripts/generate_ui_sql_export_cases.py",
        "appUrl": APP_URL,
        "backendUrl": BACKEND_URL_UI,
        "targetCaseCount": TARGET_CASES,
        "actualCaseCount": len(collected),
        "cases": collected,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {len(collected)} UI-generated SQL export cases to {OUTPUT_PATH}")
    if len(collected) < TARGET_CASES:
        raise SystemExit(
            f"Not enough unique UI-generated cases: got {len(collected)}, expected at least {TARGET_CASES}"
        )


if __name__ == "__main__":
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")
    main()
