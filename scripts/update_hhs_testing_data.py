# Updater for HHS COVID-19 Diagnostic Laboratory Testing (PCR Testing) Time Series
# https://healthdata.gov/dataset/covid-19-diagnostic-laboratory-testing-pcr-testing-time-series

import enum
import os
import pathlib

import click
import requests
import pandas as pd
import structlog

from covidactnow.datapublic import common_df
from covidactnow.datapublic import common_init
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_fields import FieldNameAndCommonField
from covidactnow.datapublic.common_fields import GetByValueMixin
from scripts import helpers

DATA_ROOT = pathlib.Path(os.path.realpath(__file__)).parent.parent / "data"
HHS_TESTING_DATA_ROOT = DATA_ROOT / "testing-hhs"
METADATA_JSON_PATH = HHS_TESTING_DATA_ROOT / "metadata.json"
DATASET_CSV_PATH = HHS_TESTING_DATA_ROOT / "covid-19_diagnostic_lab_testing.csv"
TIMESERIES_CSV_PATH = HHS_TESTING_DATA_ROOT / "timeseries-common.csv"
VERSION_PATH = HHS_TESTING_DATA_ROOT / "version.txt"

METADATA_URL = "https://healthdata.gov/api/3/action/package_show?id=c13c00e3-f3d0-4d49-8c43-bf600a6c0a0d&page=0"

_logger = structlog.getLogger()


def update_dataset_csv():
    # Fetch the JSON metadata to get the latest CSV url.
    _logger.info("Fetching metadata JSON", {"url": METADATA_URL, "path": METADATA_JSON_PATH})
    response = requests.get(METADATA_URL)
    METADATA_JSON_PATH.write_bytes(response.content)
    metadata = response.json()

    # Fetch the latest CSV.
    dataset_url = metadata["result"][0]["resources"][0]["url"]
    _logger.info("Fetching Dataset", {"url": dataset_url, "path": DATASET_CSV_PATH})
    response = requests.get(dataset_url)
    DATASET_CSV_PATH.write_bytes(response.content)

    # Update version.txt file.
    VERSION_PATH.write_text(f"Updated at {helpers.version_timestamp()} from {dataset_url}\n")


@enum.unique
class Fields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    # NOTE: There are several fields that aren't included here since we don't
    # care (e.g. fema_region) and they get dropped when we pivot the data from
    # long to wide.
    DATE = "date", CommonFields.DATE
    STATE_FIPS = "state_fips", CommonFields.FIPS
    STATE = "state", CommonFields.STATE

    # Indicates what type of data (Positive, Negative, etc.) the row represents.
    # NOTE: These get dropped after pivoting.
    OVERALL_OUTCOME = "overall_outcome", None
    TOTAL_RESULTS_REPORTED = "total_results_reported", None

    # NOTE: These columns get added after pivoting.
    POSITIVE = "Positive", CommonFields.POSITIVE_TESTS
    NEGATIVE = "Negative", CommonFields.NEGATIVE_TESTS


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms data to the common fields."""

    # We don't care about the inconclusive rows.
    df = df.loc[df[Fields.OVERALL_OUTCOME] != "Inconclusive", :]

    # We need to pivot the data to be in wide format (with positive / negative
    # as separate columns instead of as values in the overall_outcome column).
    df = df.pivot_table(
        index=[Fields.STATE_FIPS, Fields.DATE, Fields.STATE],
        columns=Fields.OVERALL_OUTCOME,
        values=Fields.TOTAL_RESULTS_REPORTED,
    )
    df = df.reset_index()

    already_transformed_fields = set()
    df = helpers.rename_fields(df, Fields, already_transformed_fields, _logger)

    # TODO(michael): For now we are disabling CA, OR, and NE due to recent outlier data.
    # We can hopefully re-enable them once we have better outlier handling.
    df = df[~df[CommonFields.STATE].isin(["CA", "OR", "NE"])]

    df[CommonFields.COUNTRY] = "USA"
    df[CommonFields.AGGREGATE_LEVEL] = "state"
    return df


@click.command()
@click.option("--replace-local-mirror/--no-replace-local-mirror", default=True)
@click.option("--generate-common-csv/--no-generate-common-csv", default=True)
def main(replace_local_mirror: bool, generate_common_csv: bool):
    common_init.configure_logging()

    if replace_local_mirror:
        update_dataset_csv()

    if generate_common_csv:
        dataset = pd.read_csv(
            DATASET_CSV_PATH,
            parse_dates=[Fields.DATE],
            dtype={Fields.STATE_FIPS: str},
            low_memory=False,
        )

        common_df.write_csv(
            transform(dataset), TIMESERIES_CSV_PATH, _logger,
        )


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
