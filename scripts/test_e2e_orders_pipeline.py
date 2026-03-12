import re
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

APP_URL = "http://localhost:1420"
BACKEND_URL_UI = "http://localhost:8000"
DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"
CUSTOMERS_CSV = DATA_DIR / "customers.csv"
ORDERS_CSV = DATA_DIR / "orders.csv"


def connect_backend(page: Page) -> None:
    page.goto(APP_URL)
    page.get_by_title("Global Settings (Connection & Appearance)").click()

    backend_opt = page.get_by_text(BACKEND_URL_UI)
    if backend_opt.count() > 0:
        backend_opt.first.click()
    else:
        page.fill("input[placeholder='http://192.168.1.10:8000']", BACKEND_URL_UI)
        page.locator("button:has(svg.lucide-plus)").click()
        page.get_by_text(BACKEND_URL_UI).click()

    page.get_by_role("button", name="Done").click()


def create_session(page: Page) -> None:
    page.locator("button:has-text('Session')").click()
    page.get_by_text("Create New Session").click()


def upload_dataset(page: Page, path: Path, dataset_name: str) -> None:
    page.locator("button[title='Import Dataset']").click()
    with page.expect_file_chooser() as fc_info:
        page.get_by_text("Click to upload").click()
    fc_info.value.set_files(str(path))

    page.locator("input[placeholder='Enter a name for this dataset']").fill(dataset_name)
    page.locator("div.fixed button").filter(has_text="Import Dataset").click()
    expect(page.get_by_text(dataset_name)).to_be_visible()


def add_data_source(page: Page, dataset_label: str) -> None:
    page.get_by_role("button", name="Add Data Source").click()
    page.get_by_text("-- Select Dataset --").last.click()
    page.get_by_role("option", name=re.compile(rf"^{dataset_label}\\b")).click()


def ensure_operation(page: Page) -> None:
    page.get_by_role("button", name="Add Child").click()
    page.get_by_text("New Operation").click()
    page.get_by_placeholder("Operation Name").fill("Orders Summary Pipeline")


def build_pipeline(page: Page) -> None:
    page.get_by_text("Add your first command").click()

    # Add remaining 5 steps
    for _ in range(5):
        page.get_by_role("button", name="Add Step").click()

    command_selects = page.locator("select").filter(has_text="Filter").filter(has_text="Join")
    command_selects.nth(0).select_option("Join")
    command_selects.nth(1).select_option("Group")
    command_selects.nth(2).select_option("Sort")
    command_selects.nth(3).select_option("Mapping")
    command_selects.nth(4).select_option("Save Variable")
    command_selects.nth(5).select_option("Complex View")

    # Join config
    page.locator("select").filter(has_text="-- Select Source --").first.select_option("orders to orders")
    page.locator("select").filter(has_text="Left Field...").first.select_option("customer_id")
    page.locator("select").filter(has_text="Right Field...").first.select_option("customer_id")

    # Force all dataset selects to orders for stable field pickers
    for sel in page.locator("select").filter(has_text="Inherit (Use Incoming Data)").all():
        sel.select_option("orders to orders")

    # Group config
    group_block = page.locator("div").filter(has_text="Group By").filter(has_text="Metrics").first
    group_block.locator("select").first.select_option("customer_id")
    group_block.locator("select").nth(1).select_option("Sum")
    group_block.locator("select").nth(2).select_option("amount")
    group_block.get_by_placeholder("As...").fill("total_amount")

    # Sort config
    sort_block = page.locator("div").filter(has_text="Asc").filter(has_text="Desc").first
    sort_block.locator("select").first.select_option("amount")
    sort_block.locator("select").nth(1).select_option("Desc")

    # Mapping config
    mapping_block = page.locator("div").filter(has_text="Add Mapping").first
    mapping_block.get_by_placeholder("Expression").fill("amount * 1.0")
    mapping_block.get_by_placeholder("Output Field").fill("amount_copy")

    # Save Variable config
    save_block = page.locator("div").filter(has_text="var_name").first
    save_block.locator("select").first.select_option("amount")
    save_block.get_by_placeholder("var_name").fill("amount_var")

    # Complex View config
    complex_block = page.locator("div").filter(has_text="Complex View Configuration").first
    complex_block.get_by_role("button", name="Add Sub-Table").click()
    complex_block.locator("select").first.select_option("customers to customers")
    complex_block.get_by_placeholder("Tab Name").fill("Customers")
    complex_block.get_by_role("button", name="Add Rule").click()
    complex_block.locator("select").filter(has_text="Sub Field...").first.select_option("customer_id")
    complex_block.locator("select").filter(has_text="Main Field...").first.select_option("customer_id")


def run_and_verify(page: Page) -> None:
    page.get_by_role("button", name="Run this operation").click()

    expect(page.get_by_text("Main Stream")).to_be_visible()
    expect(page.get_by_text("Orders Summary Pipeline")).to_be_visible()
    expect(page.get_by_text("5 Rows")).to_be_visible()
    expect(page.get_by_text("customer_id")).to_be_visible()
    expect(page.get_by_text("total_amount")).to_be_visible()
    expect(page.get_by_text("C001")).to_be_visible()


def test_orders_pipeline_complex_view(page: Page) -> None:
    connect_backend(page)
    create_session(page)

    upload_dataset(page, CUSTOMERS_CSV, "customers")
    upload_dataset(page, ORDERS_CSV, "orders")

    add_data_source(page, "customers")
    add_data_source(page, "orders")

    ensure_operation(page)
    build_pipeline(page)
    run_and_verify(page)


if __name__ == "__main__":
    print("Run with: pytest scripts/test_e2e_orders_pipeline.py")
