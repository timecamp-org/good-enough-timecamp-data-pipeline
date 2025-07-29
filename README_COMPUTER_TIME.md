# Export computer time from TimeCamp

## First run

Create `.env` file:

```bash
TIMECAMP_API_KEY={timecamp_api_token}
TIMECAMP_DOMAIN=app.timecamp.com
```

Run:

```bash
python fetch_computer_time_data.py --from 2025-07-10 --to 2025-07-12 # --user-ids "640" --debug 
python jsonl_to_csv.py timecamp_computer_time_data.jsonl # to get CSV file instead of jsonl
```

## Crontab setup

To be filled later