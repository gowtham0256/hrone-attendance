# Employee Attendance & Analytics API

FastAPI + MongoDB service for punch-in/out, manual corrections with an audit trail, and attendance
analytics built on MongoDB aggregation pipelines. All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
# set MONGO_URI and MONGO_DB (environment variables, or a local .env file; real env vars win)
uvicorn app.main:app --port 8000
```
Needs MongoDB 6.0+. Indexes are created automatically at startup. Interactive docs: http://localhost:8000/docs

Optional sample data: `python sample_seed.py`.

## Test
```bash
python tests/selftest.py          # rules R1-R5, status codes, races, analytics, explain plans
python tests/selftest.py --bulk   # also loads ~100k records and re-checks the explain plans
```
The self-test uses a separate `<MONGO_DB>_selftest` database and clears its collections when it finishes.
It compares every analytics endpoint with expected values computed independently in plain Python,
on a dataset with ties, weekend records, a mid-month joiner, a future joiner, an overnight shift,
an employee with no logs, legacy records and logs of an unknown employee.

## Notes
- Instants are epoch milliseconds in the API and BSON dates (UTC) in MongoDB; the client is `tz_aware`.
- Rounding is decimal half-up everywhere (Python `Decimal` for single values, a floor-based expression on
  `Decimal128` inside pipelines, because MongoDB's `$round` rounds half to even).
- The trend's moving average is computed over the rounded daily `attendance_rate` values.
- Business-rule 422s use the same `{"detail": [...]}` shape as FastAPI's own validation errors.
- See `REVIEW.md` for the starter defects and `DECISIONS.md` for design choices.
