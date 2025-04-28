import json
import random
from utils import MyDBUtils

def generate_iot_records(n: int) -> list:
    appliances = ["fridge", "light", "heater", "ac", "fan"]
    locations = ["lhr", "nyc", "sydney", "tokyo", "paris"]
    records = []
    for _ in range(n):
        record = {
            "appliance": random.choice(appliances),
            "power": str(random.randint(50, 500)),
            "location": random.choice(locations)
        }
        if random.random() < 0.1:
            record["ttl"] = str(random.randint(3600, 86400))
        records.append(record)
    return records

def main():
    records = generate_iot_records(10000)
    MyDBUtils.write_json("iot_data.json", records)
    print("Generated 10000 IoT records in iot_data.json")

if __name__ == "__main__":
    main()