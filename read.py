#!/usr/bin/env python3

import sys
import json
import sqlite3
import os

DB_NAME = "sup-san-reviews.db"

def read_processed_messages(date_from):
    """
    Reads messages from proc_messages where timestamp >= date_from.
    Returns a list of dicts with the records.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    query = """
    SELECT timestamp, uuid, message, category, num_lemm, num_char
    FROM proc_messages
    WHERE timestamp >= ?
    """
    rows = cur.execute(query, (date_from,)).fetchall()
    conn.close()

    messages = []
    for row in rows:
        (timestamp, uuid_val, message, category, num_lemm, num_char) = row
        messages.append({
            "timestamp": timestamp,
            "uuid": uuid_val,
            "message": message,
            "category": category,
            "num_lemm": num_lemm,
            "num_char": num_char
        })
    return messages

def main():
    if len(sys.argv) < 2:
        print("Usage: python read.py <date_from>")
        sys.exit(1)

    date_from = sys.argv[1]

    # Retrieve the records
    processed_msgs = read_processed_messages(date_from)

    # Build output structure
    output_data = {
        "num": len(processed_msgs),
        "messages": processed_msgs
    }

    # Write to JSON file
    with open("messages.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print(f"{len(processed_msgs)} messages written to messages.json")

if __name__ == "__main__":
    main()
