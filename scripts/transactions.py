"""
Simulates a bank transaction feed by inserting credit solicitudes into Neon
every ~1 second. Run this in the background to generate live data.

Usage:
    uv run python scripts/transactions.py
"""

import os
import random
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

MERCHANTS = {
    "groceries": ["Walmart", "Kroger", "Whole Foods", "Costco", "Trader Joes"],
    "electronics": ["BestBuy", "Apple Store", "Newegg", "MicroCenter", "Amazon"],
    "food": ["McDonalds", "Chipotle", "Starbucks", "Subway", "Panera"],
    "fuel": ["Shell Gas", "Chevron", "BP", "ExxonMobil"],
    "entertainment": ["Netflix", "Spotify", "AMC Theaters", "Steam"],
    "clothing": ["Nordstrom", "Target", "Zara", "H&M", "Gap"],
    "gambling": ["Online Casino", "DraftKings", "FanDuel"],
    "crypto": ["CryptoExchange", "Coinbase", "Binance"],
    "transfer": ["Wire Transfer", "Zelle", "Venmo"],
    "cash": ["Cash Advance", "ATM Withdrawal"],
    "luxury": ["Harrods", "Gucci", "Louis Vuitton"],
    "transport": ["Uber", "Lyft", "Delta Airlines"],
}

COUNTRIES = ["US", "US", "US", "US", "US", "GB", "FR", "DE", "MX", "NG", "RU", "JP"]

CATEGORIES_WEIGHTED = (
    ["groceries"] * 20
    + ["food"] * 15
    + ["fuel"] * 10
    + ["electronics"] * 10
    + ["entertainment"] * 10
    + ["clothing"] * 8
    + ["transport"] * 7
    + ["transfer"] * 5
    + ["gambling"] * 5
    + ["crypto"] * 4
    + ["cash"] * 3
    + ["luxury"] * 3
)


def get_applicant_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM credit_applications")
        return [row[0] for row in cur.fetchall()]


def generate_transaction(applicant_id):
    category = random.choice(CATEGORIES_WEIGHTED)
    merchant = random.choice(MERCHANTS[category])
    country = random.choice(COUNTRIES)
    is_online = random.random() < 0.4

    # Amount varies by category
    amount_ranges = {
        "groceries": (15, 200),
        "food": (8, 60),
        "fuel": (25, 80),
        "electronics": (50, 3000),
        "entertainment": (10, 100),
        "clothing": (20, 500),
        "gambling": (50, 5000),
        "crypto": (100, 5000),
        "transfer": (100, 10000),
        "cash": (20, 1000),
        "luxury": (200, 5000),
        "transport": (15, 800),
    }
    lo, hi = amount_ranges[category]
    amount = round(random.uniform(lo, hi), 2)

    return {
        "applicant_id": applicant_id,
        "amount": amount,
        "merchant": merchant,
        "category": category,
        "location_country": country,
        "is_online": is_online,
    }


def insert_transaction(conn, tx):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO transactions (applicant_id, amount, merchant, category, location_country, is_online)
            VALUES (%(applicant_id)s, %(amount)s, %(merchant)s, %(category)s, %(location_country)s, %(is_online)s)
            RETURNING id
            """,
            tx,
        )
        conn.commit()
        return cur.fetchone()[0]


def main():
    conn = psycopg2.connect(DATABASE_URL)
    applicant_ids = get_applicant_ids(conn)

    if not applicant_ids:
        print("No applicants found. Submit some applications first.")
        return

    print(f"Generating transactions for {len(applicant_ids)} applicants...")
    print("Press Ctrl+C to stop.\n")

    count = 0
    try:
        while True:
            applicant_id = random.choice(applicant_ids)
            tx = generate_transaction(applicant_id)
            tx_id = insert_transaction(conn, tx)
            count += 1
            print(
                f"[{count}] applicant={applicant_id} "
                f"${tx['amount']:>9,.2f} {tx['category']:<14} "
                f"{tx['merchant']:<20} {tx['location_country']}"
            )
            time.sleep(random.uniform(0.5, 1.5))
    except KeyboardInterrupt:
        print(f"\nStopped. Inserted {count} transactions.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
