import enum
import pathlib
import pandas as pd
import structlog

from covidactnow.datapublic import common_df
from covidactnow.datapublic import common_init
from covidactnow.datapublic import census_data_helpers
from covidactnow.datapublic.common_fields import CommonFields

from scripts import helpers
from scripts import ccd_helpers

Fields = ccd_helpers.Fields

DATA_URL = "https://storage.googleapis.com/us-east4-data-eng-scrapers-a02dc940-bucket/data/final/can_scrape_api_covid_us.parquet"

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
COUNTY_DATA_PATH = DATA_ROOT / "misc" / "fips_population.csv"
OUTPUT_PATH = DATA_ROOT / "testing-cdc" / "timeseries-common.csv"


def _remove_trailing_zeros(series: pd.Series) -> pd.Series:

    series = series.copy()

    index = series.loc[series != 0].last_valid_index()

    if index is None:
        # If test positivity is 0% the entire time, considering the data inaccurate, returning
        # none.
        series[:] = None
        return series

    series[index + pd.DateOffset(1) :] = None
    return series


def remove_trailing_zeros(data: pd.DataFrame) -> pd.DataFrame:
    data = data.sort_values([CommonFields.FIPS, CommonFields.DATE]).set_index(CommonFields.DATE)
    test_pos = data.groupby(CommonFields.FIPS)[CommonFields.TEST_POSITIVITY_7D].apply(
        _remove_trailing_zeros
    )
    data[CommonFields.TEST_POSITIVITY_7D] = test_pos
    return data.reset_index()


def transform(dataset: ccd_helpers.CovidCountyDataset):

    testing_df = dataset.query_variable_for_provider(
        variable_name="pcr_tests_positive",
        measurement="rolling_average_7_day",
        provider="cdc",
        unit="percentage",
    )
    fips = testing_df[Fields.LOCATION]

    # Should only be picking up county all_df for now.  May need additional logic if states
    # are included as well
    assert (testing_df[Fields.LOCATION].str.len() == 5).all()

    census_data = census_data_helpers.load_county_fips_data(COUNTY_DATA_PATH).data
    census_data = census_data.set_index(CommonFields.FIPS)
    counties = fips.map(census_data[CommonFields.COUNTY])
    states = fips.map(census_data[CommonFields.STATE])

    output_data = {
        CommonFields.FIPS: testing_df[Fields.LOCATION],
        CommonFields.DATE: testing_df[Fields.DATE],
        CommonFields.AGGREGATE_LEVEL: "county",
        CommonFields.TEST_POSITIVITY_7D: testing_df[Fields.VALUE] / 100.0,
        CommonFields.COUNTRY: "USA",
        CommonFields.COUNTY: counties,
        CommonFields.STATE: states,
    }

    data = pd.DataFrame(output_data)

    return remove_trailing_zeros(data)


if __name__ == "__main__":

    common_init.configure_logging()
    log = structlog.get_logger()

    ccd_dataset = ccd_helpers.CovidCountyDataset.load_from_url()
    all_df = transform(ccd_dataset)

    common_df.write_csv(all_df, OUTPUT_PATH, log)
