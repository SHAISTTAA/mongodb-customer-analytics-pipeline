
import pandas as pd
from pymongo import MongoClient

# -----------------------------
# Extract
# -----------------------------

def extract(path):

    print("Reading event data")

    df = pd.read_csv(path)

    return df


# -----------------------------
# Transform
# -----------------------------

def transform(df):

    print("Transforming dataset")

    df["product"] = df["product"].str.lower()

    df["city"] = df["city"].str.lower()

    df["category"] = df["category"].str.lower()

    df["revenue"] = df["price"]

    return df


# -----------------------------
# Load
# -----------------------------

def load(df):

    client = MongoClient("mongodb://localhost:27017")

    db = client["analytics"]

    collection = db["customer_events"]

    data = df.to_dict(orient="records")

    collection.insert_many(data)

    print("Data inserted into MongoDB")


# -----------------------------
# Aggregation
# -----------------------------

def run_aggregation():

    client = MongoClient("mongodb://localhost:27017")

    db = client["analytics"]

    collection = db["customer_events"]

    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "total_revenue": {"$sum": "$revenue"},
                "orders": {"$sum": 1}
            }
        }
    ]

    result = list(collection.aggregate(pipeline))

    print("\nRevenue by City")

    for r in result:
        print(r)


# -----------------------------
# Runner
# -----------------------------

def run_pipeline():

    df = extract("data/customer_events.csv")

    df = transform(df)

    load(df)

    run_aggregation()


if __name__ == "__main__":
    run_pipeline()
