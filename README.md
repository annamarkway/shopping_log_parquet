# shopping_log_parquet

A native Rust pipeline for the Shopping Sessions challenge. It cleans the event, click and link CSVs, joins linked user IDs into persons, builds sessions, attributes them to campaigns, and writes the `user_daily` and `campaign_daily` Parquet tables.

    docker build -t shopping-sessions .   # the image's CMD reads $DATA_DIR and writes $OUTPUT_DIR

Rescored build of e72fa64 (same code; this commit only touches the README).
