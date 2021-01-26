import enum
import pathlib

import click
import pandas as pd
import structlog

from covidactnow.datapublic import common_df
from covidactnow.datapublic import common_init
from covidactnow.datapublic import census_data_helpers
from covidactnow.datapublic.common_fields import CommonFields

from scripts import helpers
from scripts import ccd_helpers

Fields = ccd_helpers.Fields

DATA_URL = "https://storage.googleapis.com/can-scrape-outputs/final/can_scrape_api_covid_us.parquet"

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
COUNTY_DATA_PATH = DATA_ROOT / "misc" / "fips_population.csv"
OUTPUT_PATH = DATA_ROOT / "testing-cdc" / "timeseries-common.csv"


DC_COUNTY_FIPS = "11001"
DC_STATE_FIPS = "11"


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

    variables = [
        ccd_helpers.ScraperVariable(
            variable_name="pcr_tests_positive",
            measurement="rolling_average_7_day",
            provider="cdc",
            unit="percentage",
            common_field=CommonFields.TEST_POSITIVITY_7D,
        ),
    ]
    results = dataset.query_multiple_variables(variables)
    # Test positivity should be a ratio
    results.loc[:, CommonFields.TEST_POSITIVITY_7D] = (
        results.loc[:, CommonFields.TEST_POSITIVITY_7D] / 100.0
    )
    # Should only be picking up county all_df for now.  May need additional logic if states
    # are included as well
    assert (results[CommonFields.FIPS].str.len() == 5).all()

    # Duplicating DC County results as state results because of a downstream
    # use of how dc state data is used to override DC county data.
    dc_results = results.loc[results[CommonFields.FIPS] == DC_COUNTY_FIPS, :].copy()
    dc_results.loc[:, CommonFields.FIPS] = DC_STATE_FIPS
    dc_results.loc[:, CommonFields.AGGREGATE_LEVEL] = "state"

    results = pd.concat([results, dc_results])

    return remove_trailing_zeros(results)


@click.command()
@click.option("--fetch/--no-fetch", default=True)
def main(fetch: bool):
    common_init.configure_logging()
    log = structlog.get_logger()

    ccd_dataset = ccd_helpers.CovidCountyDataset.load(fetch=fetch)
    all_df = transform(ccd_dataset)

    common_df.write_csv(all_df, OUTPUT_PATH, log)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
