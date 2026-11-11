"""Run Spark SQL against the Hive tables and print the result.

In Docker:
  docker compose -f docker/docker-compose.yml exec spark-batch /opt/app/run.sh lake.query \\
      "SELECT pair, count(*) AS trades FROM crypto.raw_trades WHERE dt = '2026-11-10' GROUP BY pair"

Locally (from the spark/ folder):  python -m lake.query "SHOW TABLES IN crypto"
"""

import argparse

from lake.session import create_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Spark SQL against the Hive tables")
    parser.add_argument("sql", help="the statement to run, in quotes")
    parser.add_argument("--rows", type=int, default=50, help="how many rows to show (default 50)")
    args = parser.parse_args()

    spark = create_spark("crypto-query")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        spark.sql(args.sql).show(args.rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
