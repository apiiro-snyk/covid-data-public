import enum
import pathlib

import click
import pandas as pd
import structlog

from covidactnow.datapublic import common_df
from covidactnow.datapublic import common_init
from covidactnow.datapublic import census_data_helpers
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_fields import FieldNameAndCommonField
from covidactnow.datapublic.common_fields import GetByValueMixin

from scripts import helpers
from scripts import ccd_helpers


# Force SettingWithCopyWarning to raise instead of logging a warning so it is easier to find the
# code that triggered it.
pd.set_option("mode.chained_assignment", "raise")


Fields = ccd_helpers.Fields

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
COUNTY_DATA_PATH = DATA_ROOT / "misc" / "fips_population.csv"
OUTPUT_PATH = DATA_ROOT / "can-scrapers-state-providers" / "timeseries-common.csv"


def transform(dataset: ccd_helpers.CovidCountyDataset):
    variables = [
        ccd_helpers.ScraperVariable(variable_name="pcr_tests_negative", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="unspecified_tests_total", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="unspecified_tests_positive", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="icu_beds_available", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="antibody_tests_total", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="antigen_tests_positive", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="antigen_tests_negative", provider="state"),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_doses_administered", provider="state"
        ),
        ccd_helpers.ScraperVariable(variable_name="hospital_beds_in_use", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="ventilators_in_use", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="ventilators_available", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="ventilators_capacity", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="pediatric_icu_beds_in_use", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="adult_icu_beds_available", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="pediatric_icu_beds_capacity", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="unspecified_tests_negative", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="antigen_tests_total", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="adult_icu_beds_in_use", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="hospital_beds_available", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="pediatric_icu_beds_available", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="adult_icu_beds_capacity", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="icu_beds_in_use", provider="state"),
        ccd_helpers.ScraperVariable(variable_name="cases", provider="state",),
        ccd_helpers.ScraperVariable(variable_name="deaths", provider="state",),
        ccd_helpers.ScraperVariable(variable_name="hospital_beds_in_use_covid", provider="state",),
        ccd_helpers.ScraperVariable(variable_name="hospital_beds_capacity", provider="state",),
        ccd_helpers.ScraperVariable(variable_name="icu_beds_capacity", provider="state",),
        ccd_helpers.ScraperVariable(variable_name="icu_beds_in_use_covid", provider="state",),
        ccd_helpers.ScraperVariable(variable_name="pcr_tests_total", provider="state",),
        ccd_helpers.ScraperVariable(variable_name="pcr_tests_positive", provider="state",),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_allocated",
            measurement="cumulative",
            unit="doses",
            provider="state",
            common_field=CommonFields.VACCINES_ALLOCATED,
        ),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_distributed",
            measurement="cumulative",
            unit="doses",
            provider="state",
            common_field=CommonFields.VACCINES_DISTRIBUTED,
        ),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            provider="state",
            common_field=CommonFields.VACCINATIONS_INITIATED,
        ),
        ccd_helpers.ScraperVariable(
            variable_name="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            provider="state",
            common_field=CommonFields.VACCINATIONS_COMPLETED,
        ),
    ]

    results = dataset.query_multiple_variables(variables, log_provider_coverage_warnings=True)
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
