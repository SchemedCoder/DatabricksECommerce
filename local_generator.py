import os
import csv
import random
from datetime import datetime, timedelta

cities = ["Delhi", "Mumbai", "Lucknow", "Bangalore", "Hyderabad", "Pune", "Chennai"]
categories = ["Electronics", "Fashion", "Grocery", "Furniture", "Beauty", "Sports"]
payment_modes = ["UPI", "CARD", "COD", "NETBANKING"]

def generate_mock_customers(data_dir, num_customers=1000):
    """Generates mock customer profile data."""
    os.makedirs(data_dir, exist_ok=True)
    customers_file = os.path.join(data_dir, "customers.csv")
    
    with open(customers_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "customer_name", "city", "signup_date", "is_active"])
        for i in range(1, num_customers + 1):
            signup_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 1000))
            writer.writerow([
                i,
                f"Customer_{i}",
                random.choice(cities),
                signup_date.strftime("%Y-%m-%d"),
                random.choice([True, False])
            ])
    print(f"[+] Generated {num_customers} customer profiles in {customers_file}")

def generate_mock_payments(data_dir, num_payments=10000):
    """Generates mock payment transactions."""
    os.makedirs(data_dir, exist_ok=True)
    payments_file = os.path.join(data_dir, "payments.csv")
    
    with open(payments_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["payment_id", "order_id", "payment_mode", "payment_status", "payment_date"])
        for i in range(1, num_payments + 1):
            payment_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
            writer.writerow([
                i,
                i,  # 1:1 mapping with order_id for simplicity
                random.choice(payment_modes),
                random.choice(["SUCCESS", "FAILED", "PENDING"]),
                payment_date.strftime("%Y-%m-%d")
            ])
    print(f"[+] Generated {num_payments} payments in {payments_file}")

def generate_mock_orders(data_dir, num_orders=10000, start_id=1, anomaly_ratio=0.03):
    """Generates mock order data with injected anomalies (e.g. quantity <= 0, unit_price < 0, discount > 100)."""
    os.makedirs(data_dir, exist_ok=True)
    orders_file = os.path.join(data_dir, "orders.csv")
    
    with open(orders_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "order_date", "category", "quantity", "unit_price", "discount", "order_status"])
        for i in range(start_id, start_id + num_orders):
            order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10, 5000), 2)
            discount = random.choice([0.0, 5.0, 10.0, 15.0, 20.0, 0.0])
            status = random.choice(["DELIVERED", "CANCELLED", "RETURNED"])
            
            # Anomaly injection for testing quarantine engine
            if random.random() < anomaly_ratio:
                anomaly_type = random.choice(["qty", "price", "discount"])
                if anomaly_type == "qty":
                    quantity = random.choice([0, -2])
                elif anomaly_type == "price":
                    unit_price = -50.0
                elif anomaly_type == "discount":
                    discount = 120.0
                    
            writer.writerow([
                i,
                random.randint(1, 1000),  # References customer IDs
                order_date.strftime("%Y-%m-%d"),
                random.choice(categories),
                quantity,
                unit_price,
                discount,
                status
            ])
    print(f"[+] Generated {num_orders} orders in {orders_file} (Anomaly ratio: {anomaly_ratio:.1%})")

def setup_all_data(data_dir="data"):
    """Seeds the raw data files for local execution."""
    generate_mock_customers(data_dir)
    generate_mock_payments(data_dir)
    generate_mock_orders(data_dir)

if __name__ == "__main__":
    setup_all_data()
