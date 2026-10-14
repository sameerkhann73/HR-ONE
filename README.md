# Employee Attendance & Analytics API

FastAPI + MongoDB. All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
uvicorn app.main:app --port 8000
```
Needs MongoDB 6.0+ (it uses `$setWindowFields` and `$densify`). Indexes are created at startup, and `/health` returns 200 once Mongo answers.

## What is covered
Every endpoint in `openapi.yaml`, including the five explain targets. `REVIEW.md` lists the starter defects I fixed and `DECISIONS.md` answers the five questions.

## Choices where the contract left room
* `moving_avg_7d` averages the already rounded (4-decimal) `attendance_rate` values in the window, then rounds half-up again.
* The trend range limit is 92 days counting both ends; 93 days is a 422.
* PATCH uses an optimistic check. If the record changed between my read and my write, the loser gets 409 instead of overwriting the other history entry.
* A PATCH that changes nothing, comparing all seven tracked fields, is a 422.
* `punch-out` picks the latest record with `punch_in <= punched_at`; if that one is already closed it is a 409.
