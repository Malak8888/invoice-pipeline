from faker import Faker
from confluent_kafka import Producer
import json, random, time

fake = Faker()
Faker.seed(42)
random.seed(42)
p = Producer({"bootstrap.servers": "localhost:9092"})

# 100 clients fixes, chacun avec un "risque" propre
customers = [
    {"customer_id": f"C{i:03d}", "customer": fake.company(), "risk": random.uniform(0, 0.2)}
    for i in range(100)
]

print("Génération de factures... (Ctrl+C pour arrêter)")
n = 0
while True:
    c = random.choice(customers)
    amount = round(random.uniform(50, 5000), 2)
    due_days = random.choice([15, 30, 60])
    p_late = 0.05 + 0.25 * (amount / 5000) + (0.15 if due_days == 15 else 0) + c["risk"]
    invoice = {
        "invoice_id": fake.uuid4(),
        "customer_id": c["customer_id"],
        "customer": c["customer"],
        "amount": amount,
        "issued_at": fake.date_time_this_year().isoformat(),
        "due_days": due_days,
        "paid_late": random.random() < p_late,
    }
    p.produce("invoices", json.dumps(invoice))
    n += 1
    if n % 100 == 0:
        p.flush()
        print(n, "factures envoyées")
    time.sleep(0.01)