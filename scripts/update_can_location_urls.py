import pathlib

import structlog
import click
import requests
import pandas as pd
from covidactnow.datapublic import common_init
from covidactnow.datapublic import common_df
from covidactnow.datapublic.common_fields import CommonFields


DATASET_URL = (
    "https://github.com/covid-projections/covid-projections/raw/develop/src/components/"
    "MapSelectors/datasets/us_states_dataset_01_02_2020.json"
)
DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
CSV_PATH = DATA_ROOT / "misc" / "can_location_page_urls.csv"


def load_dataset():

    rows_by_fips = {}
    data = requests.get(DATASET_URL).json()

    for row in data["state_dataset"]:
        fips = row["state_fips_code"]
        row_data = {
            CommonFields.STATE: row["state_code"],
            CommonFields.COUNTRY: "USA",
            CommonFields.FIPS: fips,
            CommonFields.AGGREGATE_LEVEL: "state",
            CommonFields.CAN_LOCATION_PAGE_URL: f"https://covidactnow.org/us/{row['state_url_name']}",
        }
        rows_by_fips[fips] = row_data

    # data has structure:
    #   {"state_county_map_dataset": {"<state code>": {"county_dataset": [{...}], "dataset": ...}}}
    for state, state_data in data["state_county_map_dataset"].items():
        for row in state_data["county_dataset"]:
            state_fips = row["state_fips_code"]
            fips = row["full_fips_code"]
            state_row = rows_by_fips[state_fips]

            # County urls are nested under the state url but don't include the state url name,
            # so we need to grab the state url
            state_url = state_row[CommonFields.CAN_LOCATION_PAGE_URL]
            row_data = {
                CommonFields.STATE: row["state_code"],
                CommonFields.COUNTRY: "USA",
                CommonFields.FIPS: fips,
                CommonFields.AGGREGATE_LEVEL: "county",
                CommonFields.CAN_LOCATION_PAGE_URL: f"{state_url}/county/{row['county_url_name']}",
            }
            rows_by_fips[fips] = row_data

    return pd.DataFrame(rows_by_fips.values())


@click.command()
def main():
    common_init.configure_logging()
    log = structlog.get_logger()
    df = load_dataset()
    common_df.write_csv(df, CSV_PATH, log, index_names=[CommonFields.FIPS])


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
