"""Helpers to access and query data surfaced from the scraped Covid County Data.
"""
import enum
import dataclasses

import pandas as pd
from covidactnow.datapublic.common_fields import FieldNameAndCommonField
from covidactnow.datapublic.common_fields import GetByValueMixin
from covidactnow.datapublic.common_fields import CommonFields
from scripts import helpers

# Airflow jobs output a single parquet file with all of the data - this is where
# it is currently stored.
DATA_URL = "https://storage.googleapis.com/us-east4-data-eng-scrapers-a02dc940-bucket/data/final/can_scrape_api_covid_us.parquet"


@enum.unique
class Fields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    """Fields in CCD DataFrame."""

    PROVIDER = "provider", None
    DATE = "dt", CommonFields.DATE
    # Special transformation to FIPS
    LOCATION = "location", None
    VARIABLE_NAME = "variable_name", None
    MEASUREMENT = "measurement", None
    UNIT = "unit", None
    AGE = "age", None
    RACE = "race", None
    SEX = "sex", None
    VALUE = "value", None


@dataclasses.dataclass
class CovidCountyDataset:

    timeseries_df: pd.DataFrame

    def query_variable_for_provider(
        self, variable_name: str, measurement: str, provider: str, *, unit: str = None
    ) -> pd.DataFrame:
        """Queries a single variable for a given provider."""
        all_df = self.timeseries_df
        is_selected_data = (
            (all_df[Fields.PROVIDER] == provider)
            & (all_df[Fields.VARIABLE_NAME] == variable_name)
            & (all_df[Fields.MEASUREMENT] == measurement)
            & (all_df[Fields.AGE] == "all")
            & (all_df[Fields.RACE] == "all")
            & (all_df[Fields.SEX] == "all")
        )
        if unit:
            is_selected_data = is_selected_data & (all_df[Fields.UNIT] == unit)

        return all_df.loc[is_selected_data]

    @staticmethod
    def load_from_url(url: str = DATA_URL) -> "CovidCountyDataset":
        """Loads CovidCountyData from specficied url, performing minor cleanup."""

        all_df = pd.read_parquet(url)
        all_df[Fields.LOCATION] = helpers.fips_from_int(all_df[Fields.LOCATION])

        return CovidCountyDataset(all_df)
