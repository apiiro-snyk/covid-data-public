#!/bin/bash
# trigger-update-on-new-data.sh - Check for new cases from NYTimes and runs update + API snapshot build on success.

set -o nounset
set -o errexit

CMD=$0

# Checks command-line arguments, sets variables, etc.
prepare () {

  if [[ -z ${GITHUB_TOKEN:-} ]]; then
    echo "Error: GITHUB_TOKEN must be set to a personal access token. See:"
    echo "https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line"
    exit 1
  fi
}

execute () {
  if python scripts/update_nytimes_data.py --check-for-new-data
  then
    curl -H "Authorization: token $GITHUB_TOKEN" \
         -H "Accept: application/vnd.github.v3+json" \
         --request POST \
         --data "{ \"ref\": \"master\", \"inputs\": { \"trigger_api_build\": \"true\" } }" \
      https://api.github.com/repos/covid-projections/covid-data-model/actions/workflows/update_repo_datasets.yml/dispatches

    echo "Data sources update requested. Go to https://github.com/covid-projections/covid-data-public/actions to monitor progress."
  else
    echo "No new data found, not updating."
  fi
}

prepare "$@"
execute
