# rero-bench

## Installation

- `poetry install`
- `poetry run ./bench.py --help`

## Usage

### SQlite

- `poetry run ./bench.py  database sqlite://`

## Postgresql

- `docker run  -p 5432:5432 -e POSTGRES_PASSWORD=bench -e POSTGRES_USER=bench -e POSTGRES_DB=bench  postgres`
- `poetry run ./bench.py  database 'postgresql+psycopg2://reroils:qoDkauI(z#TAzfeZmpQ5@10.244.0.10:30014/reroils'  --number 10000`
