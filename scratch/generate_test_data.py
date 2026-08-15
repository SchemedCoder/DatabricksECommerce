import os
import csv
import random
from datetime import datetime, timedelta

def generate_data(workspace_dir):
    data_dir = os.path.join(workspace_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    customers_file = os.path.join(data_dir, "customers.csv")
    orders_file = os.path.join(data_dir, "orders.csv")
    payments_file = os.path.join(data_dir, "payments.csv")
    
    cities = ["Delhi", "Mumbai", "Lucknow", "Bangalore", "Hyderabad", "Pune", "Chennai"]
    categories = ["Electronics", "Fashion", "Grocery", "Furniture", "Beauty", "Sports"]
    payment_modes = ["UPI", "CARD", "COD", "NETBANKING"]
    
    num_customers = 1000
    num_orders = 10000
    
    # 1. customers.csv
    with open(customers_file, "w", newline="") as f:
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
            
    # 2. orders.csv
    with open(orders_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "order_date", "category", "quantity", "unit_price", "discount", "order_status"])
        for i in range(1, num_orders + 1):
            order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
            
            # Normal defaults
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(100, 5000), 2)
            discount = random.choice([0.0, 5.0, 10.0, 15.0, 20.0, 0.0])
            status = random.choice(["DELIVERED", "CANCELLED", "RETURNED"])
            
            # Anomaly injection (3% of rows to test quarantine engine)
            if i % 100 == 0:
                quantity = random.choice([0, -2])  # Invalid quantity
            elif i % 100 == 1:
                unit_price = -50.0  # Negative price
            elif i % 100 == 2:
                discount = 120.0  # Invalid discount
                
            writer.writerow([
                i,
                random.randint(1, num_customers),
                order_date.strftime("%Y-%m-%d"),
                random.choice(categories),
                quantity,
                unit_price,
                discount,
                status
            ])
            
    # 3. payments.csv
    with open(payments_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["payment_id", "order_id", "payment_mode", "payment_status", "payment_date"])
        for i in range(1, num_orders + 1):
            payment_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
            writer.writerow([
                i,
                i,
                random.choice(payment_modes),
                random.choice(["SUCCESS", "FAILED", "PENDING"]),
                payment_date.strftime("%Y-%m-%d")
            ])
            
    print(f"Generated test datasets in: {data_dir}")

if __name__ == "__main__":
    generate_data(".")
