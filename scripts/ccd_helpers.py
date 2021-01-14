"""Helpers to access and query data surfaced from the scraped Covid County Data.
"""
from typing import List
import enum
import dataclasses

import structlog
import pandas as pd
from covidactnow.datapublic.common_fields import FieldNameAndCommonField
from covidactnow.datapublic.common_fields import GetByValueMixin
from covidactnow.datapublic.common_fields import CommonFields
from scripts import helpers

# Airflow jobs output a single parquet file with all of the data - this is where
# it is currently stored.
DATA_URL = "https://storage.googleapis.com/us-east4-data-eng-scrapers-a02dc940-bucket/data/final/can_scrape_api_covid_us.parquet"


_logger = structlog.getLogger()


@enum.unique
class Fields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    """Fields in CCD DataFrame."""

    PROVIDER = "provider", None
    DATE = "dt", CommonFields.DATE
    LOCATION_TYPE = "location_type", CommonFields.AGGREGATE_LEVEL
    # Special transformation to FIPS
    LOCATION = "location", CommonFields.FIPS
    VARIABLE_NAME = "variable_name", None
    MEASUREMENT = "measurement", None
    UNIT = "unit", None
    AGE = "age", None
    RACE = "race", None
    SEX = "sex", None
    VALUE = "value", None


@dataclasses.dataclass(frozen=True)
class ScraperVariable:
    """Represents a specific variable scraped in CAN Scraper Dataset"""

    variable_name: str
    measurement: str
    provider: str
    unit: str
    age: str = "all"
    race: str = "all"
    sex: str = "all"
    common_field: CommonFields = None


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

    def query_multiple_variables(self, variables: List[ScraperVariable]) -> pd.DataFrame:
        """Queries multiple variables returning wide df with variable names as columns.

        Args:
            variable_queries: Variables to query
        """
        all_df = self.timeseries_df
        selected_data = []

        for variable in variables:

            is_selected_data = (
                (all_df[Fields.PROVIDER] == variable.provider)
                & (all_df[Fields.VARIABLE_NAME] == variable.variable_name)
                & (all_df[Fields.MEASUREMENT] == variable.measurement)
                & (all_df[Fields.AGE] == variable.age)
                & (all_df[Fields.RACE] == variable.race)
                & (all_df[Fields.SEX] == variable.sex)
            )
            if variable.unit:
                is_selected_data = is_selected_data & (all_df[Fields.UNIT] == variable.unit)

            data = all_df.loc[is_selected_data]

            # Rename fields if common field name exists
            if variable.common_field:
                data.loc[:, Fields.VARIABLE_NAME] = variable.common_field

            selected_data.append(data)

        combined_df = pd.concat(selected_data)

        wide_df = combined_df.pivot_table(
            index=[Fields.LOCATION.value, Fields.DATE.value, Fields.LOCATION_TYPE.value],
            columns=Fields.VARIABLE_NAME.value,
            values=Fields.VALUE.value,
        ).reset_index()

        data = wide_df.rename(
            columns={
                Fields.LOCATION.value: CommonFields.FIPS,
                Fields.DATE.value: CommonFields.DATE,
                Fields.LOCATION_TYPE.value: CommonFields.AGGREGATE_LEVEL,
            }
        )
        data.columns.name = None
        return data

    @staticmethod
    def load_from_url(url: str = DATA_URL) -> "CovidCountyDataset":
        """Loads CovidCountyData from specficied url, performing minor cleanup."""

        all_df = pd.read_parquet(url)
        all_df[Fields.LOCATION] = helpers.fips_from_int(all_df[Fields.LOCATION])

        return CovidCountyDataset(all_df)
