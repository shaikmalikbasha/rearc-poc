import json
import os
from io import StringIO
from typing import Dict

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import find_dotenv, load_dotenv


# =========================
# Configuration
# =========================

S3_BUCKET = "data"

POPULATION_OBJ_KEY = "population/honolulu_population_data.json"
SERIES_OBJ_KEY = "pub/time.series/pr/pr.data.0.Current"

POPULATION_START_YEAR = 2013
POPULATION_END_YEAR = 2018

TARGET_SERIES_ID = "PRS30006032"
TARGET_PERIOD = "Q01"


# =========================
# Environment & Clients
# =========================


def load_environment() -> None:
    """Load environment variables from .env file."""
    load_dotenv(find_dotenv())


def create_s3_client():
    """Create and return an S3 (or MinIO) client."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION") or "us-east-1",
    )


# =========================
# S3 Utilities
# =========================


def fetch_s3_object(s3, bucket: str, key: str) -> str:
    """Fetch an object from S3 and return its content as a string."""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    except NoCredentialsError:
        raise RuntimeError("AWS credentials not found.")

    except ClientError as exc:
        raise RuntimeError(f"Failed to fetch {key} from S3: {exc}")


# =========================
# Population Processing
# =========================


def load_population_dataframe(raw_json: str) -> pd.DataFrame:
    """Load population JSON into a cleaned Pandas DataFrame."""
    population_data = json.loads(raw_json)["data"]
    df = pd.DataFrame(population_data)

    df["Population"] = pd.to_numeric(df["Population"])
    df["Year"] = pd.to_numeric(df["Year"])

    return df


def calculate_population_stats(
    df: pd.DataFrame, start_year: int, end_year: int
) -> Dict[str, float]:
    """Calculate mean and std dev of population for a year range."""
    filtered = df[(df["Year"] >= start_year) & (df["Year"] <= end_year)]

    return {
        "mean": filtered["Population"].mean(),
        "std": filtered["Population"].std(),
    }


# =========================
# Series Processing
# =========================


def load_series_dataframe(raw_text: str) -> pd.DataFrame:
    """Load and clean time series TSV data."""
    df = pd.read_csv(StringIO(raw_text), sep="\t")

    df.columns = df.columns.str.strip()
    df["series_id"] = df["series_id"].str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def calculate_best_year_per_series(df: pd.DataFrame) -> pd.DataFrame:
    """Return the year with max summed value per series_id."""
    yearly_sum = df.groupby(["series_id", "year"], as_index=False)["value"].sum()

    return yearly_sum.loc[
        yearly_sum.groupby("series_id")["value"].idxmax()
    ].reset_index(drop=True)


# =========================
# Join & Reporting
# =========================


def join_series_with_population(
    series_df: pd.DataFrame, population_df: pd.DataFrame
) -> pd.DataFrame:
    """Join series data with population by year."""
    return series_df.merge(
        population_df,
        left_on="year",
        right_on="Year",
        how="inner",
    )


def filter_target_series(df: pd.DataFrame, series_id: str, period: str) -> pd.DataFrame:
    """Filter results for a specific series and period."""
    return df[(df["series_id"] == series_id) & (df["period"] == period)][
        ["series_id", "year", "period", "value", "Population"]
    ]


# =========================
# Main Job
# =========================


def main() -> None:
    print("🚀 Starting Population & Series Data Job")

    load_environment()
    s3 = create_s3_client()

    # --- Population ---
    population_raw = fetch_s3_object(s3, S3_BUCKET, POPULATION_OBJ_KEY)
    population_df = load_population_dataframe(population_raw)

    stats = calculate_population_stats(
        population_df,
        POPULATION_START_YEAR,
        POPULATION_END_YEAR,
    )

    print("\nUS Population Statistics (2013-2018):")
    print(f"\tMean population: {stats['mean']:,.0f}")
    print(f"\tStandard deviation: {stats['std']:,.0f}")

    # --- Series ---
    series_raw = fetch_s3_object(s3, S3_BUCKET, SERIES_OBJ_KEY)
    series_df = load_series_dataframe(series_raw)

    best_year_df = calculate_best_year_per_series(series_df)
    print(best_year_df)

    # --- Join & Filter ---
    joined_df = join_series_with_population(series_df, population_df)
    final_results = filter_target_series(
        joined_df,
        TARGET_SERIES_ID,
        TARGET_PERIOD,
    )

    print("\nFiltered Series Results:")
    print(final_results.to_string(index=False))

    print("\n✅ Job completed successfully.")


# =========================
# Entry Point
# =========================

if __name__ == "__main__":
    main()
