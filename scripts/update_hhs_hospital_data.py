import enum
import pathlib
import pandas as pd
import structlog
import us

from covidactnow.datapublic import common_df
from covidactnow.datapublic import common_init
from covidactnow.datapublic import census_data_helpers
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_fields import FieldNameAndCommonField
from covidactnow.datapublic.common_fields import GetByValueMixin

from scripts import helpers

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
COUNTY_DATA_PATH = DATA_ROOT / "misc" / "fips_population.csv"
OUTPUT_PATH = DATA_ROOT / "hospital-hhs" / "timeseries-common.csv"

DATA_URL = "https://storage.googleapis.com/us-east4-data-eng-scrapers-a02dc940-bucket/data/final/can_scrape_api_covid_us.parquet"

_logger = structlog.getLogger()

# Early data is noisy due to lack of reporting, etc.
DEFAULT_START_DATE = "2020-09-01"
CUSTOM_START_DATES = {
    "02": "2020-10-06",  # Alaska
    "04": "2020-09-02",  # Arizona
    "15": "2020-10-10",  # Hawaii
    "16": "2020-10-17",  # Idaho
    "19": "2020-09-05",  # Iowa
    "21": "2020-10-15",  # Kentucky
    "28": "2020-11-11",  # Mississippi
    "34": "2020-09-01",  # New Jersey
    "38": "2020-11-03",  # North Dakota
    "46": "2020-11-02",  # South Dakota
    "47": "2020-09-20",  # Tennessee
    "53": "2020-10-25",  # Washington
}


@enum.unique
class Fields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    PROVIDER = "provider", None
    DATE = "dt", CommonFields.DATE
    FIPS = "fips", CommonFields.FIPS
    LOCATION = "location", None
    VARIABLE_NAME = "variable_name", None
    MEASUREMENT = "measurement", None
    UNIT = "unit", None
    AGE = "age", None
    RACE = "race", None
    SEX = "sex", None
    VALUE = "value", None
    ICU_BEDS = "adult_icu_beds_capacity", CommonFields.ICU_BEDS
    ADULT_ICU_BEDS_IN_USE = "adult_icu_beds_in_use", CommonFields.CURRENT_ICU_TOTAL
    ADULT_ICU_BEDS_IN_USE_COVID = "adult_icu_beds_in_use_covid", CommonFields.CURRENT_ICU
    HOSPITAL_BEDS_CAPACITY = "hospital_beds_capacity", CommonFields.STAFFED_BEDS
    HOSPITAL_BEDS_IN_USE = "hospital_beds_in_use", CommonFields.HOSPITAL_BEDS_IN_USE_ANY
    HOSPITAL_BEDS_IN_USE_COVID = "hospital_beds_in_use_covid", CommonFields.CURRENT_HOSPITALIZED


def update(data_url: str):

    # TODO(tom): Switch to ccd_helpers. See
    #  https://github.com/covid-projections/covid-data-public/pull/196
    all_df = pd.read_parquet(data_url)

    variables = [
        "adult_icu_beds_capacity",
        "adult_icu_beds_in_use",
        "hospital_beds_capacity",
        "hospital_beds_in_use",
        "adult_icu_beds_in_use_covid",
        "hospital_beds_in_use_covid",
    ]
    unit = "beds"
    measurements = ["current", "rolling_average_7_day"]

    is_federal_hospital_data = (
        (all_df[Fields.PROVIDER] == "hhs")
        & (all_df[Fields.VARIABLE_NAME].isin(variables))
        & (all_df[Fields.MEASUREMENT].isin(measurements))
        & (all_df[Fields.UNIT] == unit)
        & (all_df[Fields.AGE] == "all")
        & (all_df[Fields.RACE] == "all")
        & (all_df[Fields.SEX] == "all")
    )

    # Subset only to hospital data we want.
    df = all_df.loc[is_federal_hospital_data].copy()

    # Add FIPS column.
    df[Fields.FIPS] = helpers.fips_from_int(df[Fields.LOCATION])

    # Subset only to columns we want.
    df = df[[Fields.FIPS, Fields.DATE, Fields.VARIABLE_NAME, Fields.VALUE]]

    # Convert to wide using variable_name as the columns.
    wide_df = df.pivot_table(
        index=[Fields.FIPS.value, Fields.DATE.value],
        columns=Fields.VARIABLE_NAME.value,
        values=Fields.VALUE.value,
    ).reset_index()

    # Rename to common fields.
    wide_df = helpers.rename_fields(wide_df, Fields, set(), _logger)

    # Split counties and states.
    counties_df = wide_df.loc[wide_df[Fields.FIPS].str.len() == 5].copy()
    states_df = wide_df.loc[wide_df[Fields.FIPS].str.len() == 2].copy()

    # Add county metadata.
    census_data = census_data_helpers.load_county_fips_data(COUNTY_DATA_PATH).data
    census_data = census_data.set_index(CommonFields.FIPS)
    counties_df[CommonFields.COUNTY] = counties_df[Fields.FIPS].map(
        census_data[CommonFields.COUNTY]
    )
    counties_df[CommonFields.STATE] = counties_df[Fields.FIPS].map(census_data[CommonFields.STATE])
    counties_df[CommonFields.AGGREGATE_LEVEL] = "county"

    # Add state metadata.
    states_df[CommonFields.STATE] = states_df[Fields.FIPS].apply(
        lambda fips: us.states.lookup(fips).abbr
    )
    states_df[CommonFields.AGGREGATE_LEVEL] = "state"

    # Merge counties and states back together.
    out_df = pd.concat([counties_df, states_df])

    # Add country metadata.
    out_df[CommonFields.COUNTRY] = "USA"

    out_df = filter_early_data(out_df)

    return out_df


def filter_early_data(df):
    keep_rows = df[CommonFields.DATE.value] >= pd.to_datetime(DEFAULT_START_DATE)
    df = df.loc[keep_rows]

    for (fips, start_date) in CUSTOM_START_DATES.items():
        keep_rows = (df[Fields.FIPS] != fips) | (
            df[CommonFields.DATE] >= pd.to_datetime(start_date)
        )
        df = df.loc[keep_rows]

    return df


if __name__ == "__main__":

    common_init.configure_logging()
    all_df = update(DATA_URL)
    common_df.write_csv(all_df, OUTPUT_PATH, _logger)
