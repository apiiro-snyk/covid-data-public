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

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
COUNTY_DATA_PATH = DATA_ROOT / "misc" / "fips_population.csv"
OUTPUT_PATH = DATA_ROOT / "vaccines-cdc" / "timeseries-common.csv"


def transform(dataset: ccd_helpers.CovidCountyDataset):

    variables = [
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_allocated",
            measurement="cumulative",
            unit="doses",
            provider="cdc",
            common_field=CommonFields.VACCINES_ALLOCATED,
        ),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_distributed",
            measurement="cumulative",
            unit="doses",
            provider="cdc",
            common_field=CommonFields.VACCINES_DISTRIBUTED,
        ),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            provider="cdc",
            common_field=CommonFields.VACCINATIONS_INITIATED,
        ),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            provider="cdc",
            common_field=CommonFields.VACCINATIONS_COMPLETED,
        ),
    ]

    results = dataset.query_multiple_variables(variables)
    return results


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
