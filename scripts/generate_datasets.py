
import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

OUTPUT_DIR = "test_data"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def generate_ecommerce_orders(n=100):
    customers = [f"CUST_{i:03d}" for i in range(1, 21)]
    statuses = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]

    data = {
        "order_id": [f"ORD_{i:04d}" for i in range(1, n+1)],
        "customer_id": [random.choice(customers) for _ in range(n)],
        "amount": np.round(np.random.uniform(10, 500, n), 2),
        "status": [random.choice(statuses) for _ in range(n)],
        "order_date": [datetime.now() - timedelta(days=random.randint(0, 365)) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "ecommerce_orders.csv"), index=False)
    print("Generated ecommerce_orders.csv")

def generate_hr_employees(n=50):
    depts = ["Engineering", "HR", "Sales", "Marketing", "Finance"]

    data = {
        "emp_id": [f"EMP_{i:03d}" for i in range(1, n+1)],
        "name": [f"Employee_{i}" for i in range(1, n+1)],
        "department": [random.choice(depts) for _ in range(n)],
        "salary": np.random.randint(40000, 150000, n),
        "join_date": [datetime.now() - timedelta(days=random.randint(100, 2000)) for _ in range(n)],
        "is_active": [random.choice([True, False]) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "hr_employees.csv"), index=False)
    print("Generated hr_employees.csv")

def generate_iot_logs(n=200):
    sensors = [f"SENS_{i:02d}" for i in range(1, 11)]
    locations = ["Factory_A", "Factory_B", "Warehouse_C"]

    data = {
        "log_id": range(1, n+1),
        "sensor_id": [random.choice(sensors) for _ in range(n)],
        "location": [random.choice(locations) for _ in range(n)],
        "temperature": np.round(np.random.normal(25, 5, n), 1),
        "humidity": np.random.randint(30, 90, n),
        "timestamp": [datetime.now() - timedelta(minutes=random.randint(0, 10000)) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "iot_logs.csv"), index=False)
    print("Generated iot_logs.csv")

def generate_financial_ledger(n=150):
    accounts = [f"ACC_{i:03d}" for i in range(1, 15)]
    types = ["DEBIT", "CREDIT"]

    data = {
        "tx_id": [f"TX_{i:05d}" for i in range(1, n+1)],
        "account_id": [random.choice(accounts) for _ in range(n)],
        "tx_type": [random.choice(types) for _ in range(n)],
        "amount": np.round(np.random.uniform(100, 10000, n), 2),
        "currency": ["USD"] * n,
        "is_audited": [random.choice([True, False]) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "financial_ledger.csv"), index=False)
    print("Generated financial_ledger.csv")

def generate_student_scores(n=100):
    students = [f"STU_{i:03d}" for i in range(1, 21)]
    subjects = ["Math", "Physics", "Chemistry", "History", "Art"]

    data = {
        "record_id": range(1, n+1),
        "student_id": [random.choice(students) for _ in range(n)],
        "subject": [random.choice(subjects) for _ in range(n)],
        "score": np.random.randint(50, 100, n),
        "semester": [random.choice(["Fall_2023", "Spring_2024"]) for _ in range(n)]
    }
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "student_scores.csv"), index=False)
    print("Generated student_scores.csv")

def generate_inventory_items(n=50):
    warehouses = ["North_WH", "South_WH", "East_WH"]
    suppliers = [f"SUP_{i:02d}" for i in range(1, 6)]

    data = {
        "item_id": [f"ITEM_{i:03d}" for i in range(1, n+1)],
        "warehouse": [random.choice(warehouses) for _ in range(n)],
        "stock_qty": np.random.randint(0, 500, n),
        "supplier_id": [random.choice(suppliers) for _ in range(n)],
        "reorder_level": np.random.randint(10, 50, n)
    }
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "inventory_items.csv"), index=False)
    print("Generated inventory_items.csv")

if __name__ == "__main__":
    generate_ecommerce_orders()
    generate_hr_employees()
    generate_iot_logs()
    generate_financial_ledger()
    generate_student_scores()
    generate_inventory_items()
