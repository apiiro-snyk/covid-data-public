#!/bin/bash
set -o nounset
set -o errexit

python scripts/update.py
python scripts/update_covid_tracking_data.py
# TODO(brett): Change CCM to static file from 2018 Hospital Survey
# python scripts/update_covid_care_map.py

# TODO(chris): Potentially remove NHA scraper (https://trello.com/c/MXlquRDc/600-remove-nha-scraper-in-covid-data-public)
# python scripts/update_nha_hospitalization_county.py

python scripts/update_nytimes_data.py
python scripts/update_test_and_trace.py
# TODO(https://trello.com/c/PeQXdUCU): Fix Texas hospitalizations.
python scripts/update_texas_tsa_hospitalizations.py || echo "Failed to update Texas Hospitals"
python scripts/update_texas_fips_hospitalizations.py
python scripts/update_forecast_hub.py
python scripts/update_covid_county_data.py
# AWS Lake seems to be hanging the build right now.
# python scripts/update_aws_lake.py --replace-local-mirror --cleanup-local-mirror
python scripts/update_hhs_testing_data.py
# TODO(michael): Make this non-fatal once we have more trust and are relying on
# this data.
python scripts/update_cms_testing_data.py || echo "Failed to update CMS Test Positivity data."
