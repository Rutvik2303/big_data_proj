from kafka import KafkaProducer
import json
import csv
import time

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='ip-172-31-6-42.eu-west-2.compute.internal:9092,ip-172-31-3-85.eu-west-2.compute.internal:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

file_path = "../data/test.csv"

with open(file_path, mode='r') as file:
    reader = csv.DictReader(file)

    for row in reader:
        # Convert types to match Cassandra table
        data = {
            "order_id": int(row.get("order_id") or 0),
            "order_item_id": int(row.get("order_item_id") or 0),
            "commission_amount": float(row.get("commission_amount") or 0.0),
            "customer_id": row.get("customer_id") or "",
            "discount_percent": float(row.get("discount_percent") or 0.0),
            "estimated_delivery_end": row.get("estimated_delivery_end") or "",
            "estimated_delivery_start": row.get("estimated_delivery_start") or "",
            "is_campaign": bool(int(row.get("is_campaign") or 0)),
            "item_status": row.get("item_status") or "",
            "line_total": float(row.get("line_total") or 0.0),
            "maintenance_amount": float(row.get("maintenance_amount") or 0.0),
            "order_date": row.get("order_date") or "",
            "product_campaign_id": float(row.get("product_campaign_id") or 0.0),
            "product_id": row.get("product_id") or "",
            "quantity": int(row.get("quantity") or 0),
            "shipping_fee_item": float(row.get("shipping_fee_item") or 0.0),
            "unit_price": float(row.get("unit_price") or 0.0),
            "unit_price_after_discount": float(row.get("unit_price_after_discount") or 0.0)
        }

        # Send data to Kafka
        producer.send("test_shopee", value=data)
        time.sleep(2)
        print(f"Sent order_id={row.get('order_item_id')} to Kafka \n")

# Ensure all messages are sent
producer.flush()
print("Entire CSV file sent to Kafka ✅")