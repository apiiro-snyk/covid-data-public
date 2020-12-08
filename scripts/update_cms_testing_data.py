"""Updater for CMS county-level Test Positivity Rates.

Imports county-level test positivity from CMS (Centers for Medicare & Medicaid Services).
This is the only comprehensive county-level test positivity data source we have, but
unfortunately it only contains weekly data, and can be 7-10 days stale. The data is also
not super accessible (have to scrape HTML) or clean (column names have changed from one
week to the next, etc.)

Archive Index (HTML): https://data.cms.gov/stories/s/q5r5-gjyu

We scrape the archive page to find all the historical datasets and then
download / import / merge them to form our timeseries data.

There are a couple alternative approaches we could consider if this ends up
fragile:
 1. The current dataset is available at a predictable URL
    (https://data.cms.gov/download/hsg2-yqzz/application%2Fzip). We could
    fetch it and then merge it into a "running" timeseries CSV that includes
    prior data. But this seems fragile since we wouldn't be able to
    reconstruct the running timeseries from scratch if anything got
    out-of-sync.
 2. We could try to query the historical datasets via the data.cms.gov
    catalog API, e.g.  https://data.cms.gov/api/catalog/v1?q=Positivity but
    they're not annotated in a predictable way, so I think it'd be more
    fragile.
"""

from datetime import datetime
import enum
import os
import pathlib
import re
import zipfile

from bs4 import BeautifulSoup
import click
import numpy as np
import pandas as pd
import requests
import structlog

from covidactnow.datapublic import common_df
from covidactnow.datapublic import common_init
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_fields import FieldNameAndCommonField
from covidactnow.datapublic.common_fields import GetByValueMixin
from scripts import helpers

DATA_ROOT = pathlib.Path(os.path.realpath(__file__)).parent.parent / "data"
CMS_TESTING_DATA_ROOT = DATA_ROOT / "testing-cms"
ARCHIVE_DATASETS_PATH = CMS_TESTING_DATA_ROOT / "archive"
ARCHIVE_INDEX_HTML_PATH = ARCHIVE_DATASETS_PATH / "index.html"
TIMESERIES_CSV_PATH = CMS_TESTING_DATA_ROOT / "timeseries-common.csv"
VERSION_PATH = CMS_TESTING_DATA_ROOT / "version.txt"

ARCHIVE_INDEX_HTML_URL = "https://data.cms.gov/stories/s/q5r5-gjyu"

_logger = structlog.getLogger()


def update_datasets():
    ARCHIVE_DATASETS_PATH.mkdir(parents=True, exist_ok=True)
    # Since we re-download all archive datasets every time, go ahead and delete anything we have
    # in git, since we've seen errant ones appear and disappear before.
    for file in ARCHIVE_DATASETS_PATH.iterdir():
        file.unlink()

    _logger.info(
        "Fetching datasets archive html page",
        {"url": ARCHIVE_INDEX_HTML_URL, "local_path": ARCHIVE_INDEX_HTML_PATH},
    )
    response = requests.get(ARCHIVE_INDEX_HTML_URL)
    ARCHIVE_INDEX_HTML_PATH.write_bytes(response.content)
    page = BeautifulSoup(response.text, "html.parser")
    links = page.find_all("a")
    datasets = [
        (link.string, link["href"]) for link in links if "data.cms.gov/download" in link.get("href")
    ]

    # Download them all.
    for name, url in datasets:
        # Extract the dataset date from the name extracted from the link
        # and use it as the destination filename.
        date_string = re.match(".*Week Ending (.*)", name).group(1)
        date = datetime.strptime(date_string, "%m/%d/%y").date()
        dest_file_name = date.strftime("%Y-%m-%d") + ".zip"
        dest_path = ARCHIVE_DATASETS_PATH / dest_file_name

        _logger.info("Fetching dataset", {"url": url, "dest": dest_path})
        response = requests.get(url)
        dest_path.write_bytes(response.content)

    # Update version.txt file.
    VERSION_PATH.write_text(f"Updated at {helpers.version_timestamp()}\n")


@enum.unique
class Fields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    COUNTY = "County", CommonFields.COUNTY
    FIPS_CODE = "FIPS Code", CommonFields.FIPS
    STATE = "State", CommonFields.STATE
    FEMA_REGION = "FEMA Region", None
    POPULATION = "Population", None
    URBAN_RURAL = "NCHS Urban Rural Classification", None
    TESTS_14D = "Tests in prior 14 days", None
    TESTS_14D_NORMALIZED = "14-day test rate per 100,000 population", None
    TEST_POSITIVITY = "Percent Positivity in prior 14 days", CommonFields.TEST_POSITIVITY_14D
    POSITIVITY_CLASSIFICATION = "Test Positivity Classification - 14 days", None


def transform_cms_datasets() -> pd.DataFrame:
    """Reads the per-date CMS datasets and transforms / merges them into a single "common" DataFrame."""

    common_dataframes = []
    for dataset_zip in filter(lambda f: f.endswith(".zip"), os.listdir(ARCHIVE_DATASETS_PATH)):
        _logger.info("Parsing dataset", {"file": dataset_zip})
        date_string = re.match("(.*).zip", dataset_zip).group(1)
        date = datetime.strptime(date_string, "%Y-%m-%d").date()
        df = read_cms_dataset_from_zip(ARCHIVE_DATASETS_PATH / dataset_zip)
        common_dataframes.append(transform_cms_dataset(date, df))

    return pd.concat(common_dataframes)


def read_cms_dataset_from_zip(zip_path: pathlib.Path) -> pd.DataFrame:
    """Finds and reads the Excel file within the CMS dataset zip file."""
    zip = zipfile.ZipFile(zip_path)
    excel_files = [
        f for f in zip.filelist if f.filename.endswith(".xlsx") and "__MACOSX" not in f.filename
    ]
    assert len(excel_files) == 1

    # HACK: The excel file has some "header" rows at the top before the columns are defined.
    # Unfortunately, the number of header rows has changed over time, and so we don't know how
    # many there will be. So we try parsing the file repeatedly with different numbers of header
    # rows until we successfully parse out a "County" column, which should always be present.
    for headers in range(5, 8):
        df = pd.read_excel(zip.read(excel_files[0]), header=headers)
        if "County" in df.columns:
            break
    else:
        raise AssertionError("Failed to read data out of excel file in " + str(zip_path))

    return df


def transform_cms_dataset(date: datetime.date, df: pd.DataFrame) -> pd.DataFrame:
    """Transforms a CMS dataset DataFrame into a "common" DataFrame (for a particular date."""

    # A few columns had different names in older data sets. We try to fix them all
    # (though we only really care about FIPS and TEST_POSITIVITY)
    df = df.rename(
        columns={
            "FIPS": Fields.FIPS_CODE.value,
            "FIPS code": Fields.FIPS_CODE.value,
            "14-day test rate": Fields.TESTS_14D.value,
            "14-day test rate per 100,000": Fields.TESTS_14D_NORMALIZED.value,
            # NOTE: This is conflating 7-day and 14-day data, but only the very oldest data
            # set has the 7-day data, so not too worried about it.
            "Percent Positive in prior 7 days": Fields.TEST_POSITIVITY.value,
            "FEMA region": Fields.FEMA_REGION.value,
            "Test Positivity Classification": Fields.POSITIVITY_CLASSIFICATION,
        }
    )

    # Make sure FIPS is a 0-padded string.
    df[Fields.FIPS_CODE.value] = df[Fields.FIPS_CODE.value].map(lambda fips: f"{fips:05}")

    # Remove non-numeric test positivity entries (e.g. "<10 tests")
    df[Fields.TEST_POSITIVITY.value] = df[Fields.TEST_POSITIVITY.value].map(
        lambda pct: pct if type(pct) == float else np.nan
    )

    # Rename to common fields.
    # NOTE: This will raise some warnings because some of the older datasets
    # don't have all the columns.
    already_transformed_fields = set()
    df = helpers.rename_fields(df, Fields, already_transformed_fields, _logger)
    df[CommonFields.COUNTRY] = "USA"
    df[CommonFields.AGGREGATE_LEVEL] = "county"

    # Add date
    df[CommonFields.DATE] = date

    return df


@click.command()
@click.option("--replace-local-mirror/--no-replace-local-mirror", default=True)
@click.option("--generate-common-csv/--no-generate-common-csv", default=True)
def main(replace_local_mirror: bool, generate_common_csv: bool):
    common_init.configure_logging()

    if replace_local_mirror:
        update_datasets()

    if generate_common_csv:
        common_df.write_csv(transform_cms_datasets(), TIMESERIES_CSV_PATH, _logger)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
