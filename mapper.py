#!/usr/bin/env python3

import sys

# Assuming the matching field is the first column in each CSV file
matching_field_index = 0

for line in sys.stdin:
    # Remove leading/trailing whitespaces and split the line by comma
    fields = line.strip().split(',')

    # Extract the matching field value
    matching_field = fields[matching_field_index]

    # Emit the matching field value as the key and the entire record as the value
    print(f"{matching_field}\t{','.join(fields)}")




