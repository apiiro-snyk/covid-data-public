import datetime
import enum
import pathlib

import pandas as pd
import click
import pytz
import requests
import structlog

from covidactnow.datapublic import common_df
from covidactnow.datapublic import common_init
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_fields import FieldNameAndCommonField
from covidactnow.datapublic.common_fields import GetByValueMixin
from scripts import helpers
import us.states

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
COVID_CARE_MAP_ROOT = DATA_ROOT / "covid-care-map"
STATIC_CSV_PATH = COVID_CARE_MAP_ROOT / "static.csv"

STATE_TO_FIPS = us.states.mapping(from_field="abbr", to_field="fips")


@enum.unique
class StateFields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    STATE = "State", CommonFields.STATE
    STAFFED_ALL_BEDS = "Staffed All Beds", CommonFields.STAFFED_BEDS
    STAFFED_ICU_BEDS = "Staffed ICU Beds", CommonFields.ICU_BEDS
    LICENSED_ALL_BEDS = "Licensed All Beds", CommonFields.LICENSED_BEDS
    ALL_BED_TYPICAL_OCCUPANCY_RATE = (
        "All Bed Occupancy Rate",
        CommonFields.ALL_BED_TYPICAL_OCCUPANCY_RATE,
    )
    ICU_TYPICAL_OCCUPANCY_RATE = "ICU Bed Occupancy Rate", CommonFields.ICU_TYPICAL_OCCUPANCY_RATE


@enum.unique
class CountyFields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    FIPS = "fips_code", CommonFields.FIPS
    STATE = "State", CommonFields.STATE
    COUNTY = "County Name", CommonFields.COUNTY
    STAFFED_ALL_BEDS = "Staffed All Beds", CommonFields.STAFFED_BEDS
    STAFFED_ICU_BEDS = "Staffed ICU Beds", CommonFields.ICU_BEDS
    LICENSED_ALL_BEDS = "Licensed All Beds", CommonFields.LICENSED_BEDS
    ALL_BED_TYPICAL_OCCUPANCY_RATE = (
        "All Bed Occupancy Rate",
        CommonFields.ALL_BED_TYPICAL_OCCUPANCY_RATE,
    )
    ICU_TYPICAL_OCCUPANCY_RATE = "ICU Bed Occupancy Rate", CommonFields.ICU_TYPICAL_OCCUPANCY_RATE


class CovidCareMapUpdater(object):
    """Updates the covid care map data."""

    COUNTY_DATA_URL = (
        "https://raw.githubusercontent.com/covidcaremap/covid19-healthsystemcapacity/"
        "master/data/published/us_healthcare_capacity-county-CovidCareMap.csv"
    )
    STATE_DATA_URL = (
        "https://raw.githubusercontent.com/covidcaremap/covid19-healthsystemcapacity/"
        "master/data/published/us_healthcare_capacity-state-CovidCareMap.csv"
    )

    @property
    def output_path(self) -> pathlib.Path:
        return COVID_CARE_MAP_ROOT / "healthcare_capacity_data_county.csv"

    @property
    def state_output_path(self) -> pathlib.Path:
        return COVID_CARE_MAP_ROOT / "healthcare_capacity_data_state.csv"

    @property
    def version_path(self) -> pathlib.Path:
        return COVID_CARE_MAP_ROOT / "version.txt"

    @staticmethod
    def _stamp():
        pacific = pytz.timezone("UTC")
        d = datetime.datetime.now(pacific)
        return d.strftime("%A %b %d %I:%M:%S %p %Z")

    def update(self):
        structlog.get_logger().info("Updating Covid Care Map data.")
        response = requests.get(self.COUNTY_DATA_URL)
        self.output_path.write_bytes(response.content)
        response = requests.get(self.STATE_DATA_URL)
        self.state_output_path.write_bytes(response.content)

        version_path = self.version_path
        version_path.write_text(f"Updated at {self._stamp()}\n")

    def transform(self) -> pd.DataFrame:
        log = structlog.get_logger()

        state_df = pd.read_csv(self.state_output_path)
        state_df = helpers.rename_fields(
            state_df, StateFields, set(), log, check_extra_fields=False
        )
        state_df[CommonFields.FIPS] = state_df[CommonFields.STATE].map(STATE_TO_FIPS)
        state_df[CommonFields.AGGREGATE_LEVEL] = "state"

        county_df = pd.read_csv(self.output_path, dtype={CountyFields.FIPS: str})
        county_df = helpers.rename_fields(
            county_df, CountyFields, set(), log, check_extra_fields=False
        )
        county_df[CommonFields.AGGREGATE_LEVEL] = "county"

        all_df = pd.concat([county_df, state_df])
        dups = all_df.duplicated(CommonFields.FIPS)
        if dups.any():
            raise ValueError(f"Unexpected duplicate fips\n{all_df.loc[dups, :]}")

        all_df = all_df.set_index([CommonFields.FIPS], verify_integrity=True)

        all_df[CommonFields.COUNTRY] = "USA"

        # Override Washoe County ICU capacity with actual numbers.
        all_df.at["32031", CommonFields.ICU_BEDS] = 162

        # Overriding NV ICU capacity numbers with actuals
        all_df.at[STATE_TO_FIPS["NV"], CommonFields.ICU_BEDS] = 844

        # According to the Utah Department of Health, they only have possible staff for
        # 85% of the ICU bed listed. This reproduces
        # https://github.com/covid-projections/covid-data-public/commit/be79d85e1aaa058f4c5d0a472c84047b59d7f3d8
        all_df.at[STATE_TO_FIPS["UT"], CommonFields.ICU_BEDS] = 564

        all_df[CommonFields.MAX_BED_COUNT] = all_df[
            [CommonFields.STAFFED_BEDS, CommonFields.LICENSED_BEDS]
        ].max(axis=1)

        # The virgin islands do not currently have associated fips codes.
        # if VI is supported in the future, this should be removed.
        is_virgin_islands = all_df[CommonFields.STATE] == "VI"
        return all_df.loc[~is_virgin_islands, :]


@click.command()
@click.option("--fetch/--no-fetch", default=False)
def main(fetch: bool):
    common_init.configure_logging()
    log = structlog.get_logger()
    updater = CovidCareMapUpdater()
    if fetch:
        updater.update()

    df = updater.transform()
    common_df.write_csv(df, STATIC_CSV_PATH, log, [CommonFields.FIPS])


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
