# Export TimeCamp data to Google BigQuery

The script loads selected TimeCamp datasets into tables in the BigQuery dataset
`timecamp`. You need a TimeCamp API key in `.env` (see `.env.sample`) and a Google
Cloud project with the BigQuery API enabled. Grant the identity running the
script permission to edit BigQuery data and create jobs. The [dlt BigQuery setup
guide](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery) lists the
BigQuery Data Editor, BigQuery Job User, and BigQuery Read Session User roles.

Authenticate with [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc).
For a service account JSON file, set:

```sh
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/service-account.json"
export DESTINATION__BIGQUERY__PROJECT_ID="your-google-cloud-project-id"
export DESTINATION__BIGQUERY__LOCATION="EU"  # Use the location you want for the dataset; default is US.
```

Keep the credential file outside this repository. `dlt` also accepts credentials
from `gcloud auth application-default login` or a Google Cloud runtime identity.
`DESTINATION__BIGQUERY__PROJECT_ID` chooses the project that receives the data.

Run the export from the repository root:

```sh
uv run --with-requirements requirements.txt --with 'dlt[bigquery]' \
   dlt_fetch_timecamp.py \
   --destination bigquery \
   --from yesterday --to yesterday \
   --datasets entries,tasks,users,computer_activities,application_names
```

Without `--format`, dlt chooses BigQuery's preferred JSONL format;
`--format parquet` also works. BigQuery does not support CSV loads through dlt.
`--output` applies only to filesystem exports. A successful run reports the dlt
load result. Check the tables in BigQuery with a query such as:

```sql
SELECT COUNT(*) FROM `your-google-cloud-project-id.timecamp.entries`;
```

**Each selected table is replaced on every run.** For example, exporting only
yesterday's `entries` replaces the previous `entries` table with yesterday's
records. To retain a longer range, pass that whole range on each run. `tasks`
and `users` fetch the current state; `application_names` is derived from
computer activities in the requested date range.

Add `--custom-fields` to also load the `entries_custom_fields`,
`tasks_custom_fields` and `users_custom_fields` tables. Each row is one set
value (`resource_id`, `template_id`, `name`, `field_type`, `value`); unset
template defaults are not included. They also contain the current state, so
`entries_custom_fields` has the values of all entries, not only the requested
date range. `users.user_id` is a `STRING` column, so cast it when you join user
custom fields:

```sql
SELECT u.email, cf.name, cf.value
FROM `your-google-cloud-project-id.timecamp.users` u
JOIN `your-google-cloud-project-id.timecamp.users_custom_fields` cf
  ON cf.resource_id = CAST(u.user_id AS INT64);
```
