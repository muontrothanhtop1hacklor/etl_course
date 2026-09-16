# QTTG output evidence

This directory contains small, reviewable samples committed to GitHub to prove
that the local Spark pipeline produced Bronze and Silver outputs.

- `bronze_master_sample.csv`: representative Bronze master rows
- `bronze_detail_sample.csv`: representative Bronze detail rows
- `silver_master_sample.csv`: representative Silver master rows
- `silver_detail_sample.csv`: representative Silver detail rows
- `quarantine_detail_sample.csv`: representative rejected detail rows

The complete Parquet datasets remain local and are intentionally ignored by Git.
The row counts and quality checks are recorded in
`docs/results/qttg-output-evidence.md`.