# customer-macro

A dbt project.

## Setup

```bash
# 1. Install dbt (use a virtualenv)
python -m venv .venv
source .venv/bin/activate
pip install dbt-snowflake  # or dbt-redshift / dbt-bigquery

# 2. Install dbt packages
dbt deps

# 3. Configure credentials
#    Either copy profiles.yml to ~/.dbt/ and fill in, or export env vars:
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_DATABASE=...
export SNOWFLAKE_WAREHOUSE=...

# 4. Verify connection
dbt debug --profiles-dir .

# 5. Run models
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Layout

```
models/        SQL models
macros/        Jinja macros
tests/         Singular + generic tests
seeds/         CSV seed data
snapshots/     Snapshot definitions
analyses/      Ad-hoc analyses
```
