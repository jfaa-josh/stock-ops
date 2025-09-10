import random

from stockops.config import config

def main():
    data_dir = config.DATA_RAW_DIR/'inputs'

    input_file = data_dir/'xformer_out_test_data.txt'
    target_file = data_dir/'test_data.txt'

    with open(input_file, 'r') as f:
        txt = f.readlines()

    new_rows = []
    for row in txt:
        if row.startswith("{'db_path"):
            new_rows.append(row)

    random.shuffle(new_rows)

    with open(target_file, "w", encoding="utf-8") as f:
        for x in new_rows:
            f.write(f"{x}\n")   # ensures a newline per element

if __name__ == "__main__":
    main()

"""
NOTE: xformer_out_test_data.txt contains:
- all 4 producer types for EODHD data.
- Historical intraday spans 2 days
- There is one duplicated entry (1755526670499 ts for streaming SPY).
- There is one historical interday entry with a duplicated timestamp, with an updated value for open
- There is one historical intraday intry with Nonetype data (should not be stored in .db)
- There is one historical interday with a different ticker
- There is one set of quotes and prices streaming data at the same timestamp
"""
