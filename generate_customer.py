import json
from faker import Faker
import random

# Initialize Faker
fake = Faker()

def generate_customer(customer_id):
    return {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": str(random.randint(1000000000, 9999999999)),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "zipcode": fake.zipcode()
        },
        "account": {
            "account_number": fake.bban(),
            "account_type": random.choice(["Savings", "Current", "Business"]),
            "balance": round(random.uniform(1000, 500000), 2),
            "currency": "INR"
        },
        "created_at": fake.iso8601()
    }

def generate_customers(num_records):
    customers = []
    for i in range(1, num_records + 1):
        customers.append(generate_customer(i))
    return customers

if __name__ == "__main__":
    num_records = 10000
    output_file = "customers.json"

    data = generate_customers(num_records)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"{num_records} customer records generated in {output_file}")