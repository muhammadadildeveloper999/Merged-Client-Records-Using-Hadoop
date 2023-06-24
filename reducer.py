#!/usr/bin/env python3

import sys

current_key = None
current_records = []

for line in sys.stdin:
    key, record = line.strip().split('\t')

    if current_key is None:
        current_key = key

    if key != current_key:
        # Process the records with the previous key
        if len(current_records) > 1:
            merged_record = ','.join(current_records)
            print(merged_record)
        else:
            # Create a new record with the unmatched key
            new_record = current_records[0]
            print(new_record)

        # Reset for the new key
        current_key = key
        current_records = []

    current_records.append(record)

# Process the last set of records
if len(current_records) > 1:
    merged_record = ','.join(current_records)
    print(merged_record)
else:
    # Create a new record with the unmatched key
    new_record = current_records[0]
    print(new_record)