"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env fills in missing values; real environment variables win)

Layout of this file:
  1. Configuration and MongoDB client
  2. Time helpers and business rules R1-R5
  3. Serialisation (BSON <-> API) and validation helpers
  4. Request models
  5. Index creation at startup
  6. Employee and attendance endpoints
  7. Aggregation pipelines (shared by the analytics endpoints and /admin/explain)
  8. Analytics endpoints
  9. Explain endpoint
"""
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration and MongoDB client
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # .env only fills gaps; variables already in the environment take priority

MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB")
if not MONGO_URI or not MONGO_DB:
    raise RuntimeError("MONGO_URI and MONGO_DB must be set (environment or .env)")

# tz_aware=True: PyMongo returns timezone-aware UTC datetimes instead of naive ones, so arithmetic
# with IST shift times is always correct and never mixes naive and aware values.
client = MongoClient(MONGO_URI, tz_aware=True, tzinfo=timezone.utc, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]
employees_col = db["employees"]
logs_col = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]
ABSENCE_STATUSES = ["ABSENT", "LEAVE"]
GRACE_SECONDS = 10 * 60          # R2
MIN_OVERTIME_MINUTES = 30        # R3
HALF_DAY_HOURS = 4.5             # R5
MAX_SHIFT = timedelta(hours=24)  # punch-out must be within 24 h of punch-in
MIN_EPOCH_MS = 100_000_000_000   # anything smaller looks like seconds
MAX_EPOCH_MS = 4_102_444_800_000
MAX_TREND_DAYS = 92

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"

StatusName = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceName = Literal["PRESENT", "WFH", "ON_DUTY"]


# --------------------------------------------------------------------------- #
# 2. Time helpers and business rules
# --------------------------------------------------------------------------- #
def now_ms() -> int:
    return (datetime.now(UTC) - EPOCH) // timedelta(milliseconds=1)


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    """Aware (or naive, assumed UTC) datetime -> epoch ms. Integer maths, no float rounding."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return (value - EPOCH) // timedelta(milliseconds=1)


def parse_hhmm(value: str) -> time:
    hours, minutes = value.split(":")
    return time(int(hours), int(minutes))


def is_overnight(emp: dict) -> bool:
    return emp["shift_end"] <= emp["shift_start"]  # zero-padded HH:MM compares correctly as strings


def shift_window(day: date, emp: dict) -> tuple[datetime, datetime]:
    """Shift start and end instants for the shift that starts on `day` (IST). Overnight ends next day."""
    start = datetime.combine(day, parse_hhmm(emp["shift_start"]), IST)
    end = datetime.combine(day, parse_hhmm(emp["shift_end"]), IST)
    if is_overnight(emp):
        end += timedelta(days=1)
    return start, end


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: IST calendar date of the punch-in; early punch-ins of an overnight shift belong to the previous day."""
    local = punch_in.astimezone(IST)
    if is_overnight(emp) and local.time() < parse_hhmm(emp["shift_end"]):
        return local.date() - timedelta(days=1)
    return local.date()


def late_minutes(punch_in: datetime, emp: dict, day: date) -> int:
    """R2: late only when strictly more than 10:00 after shift start; minutes counted from shift start."""
    start, _ = shift_window(day, emp)
    seconds = (punch_in - start) // timedelta(seconds=1)
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def overtime_minutes(punch_out: datetime, emp: dict, day: date) -> int:
    """R3: whole minutes after shift end, only when at least 30."""
    _, end = shift_window(day, emp)
    minutes = (punch_out - end) // timedelta(minutes=1)
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def round_half_up(value, places: int) -> Optional[float]:
    """R4/R8: decimal half-up rounding (Python's round() is banker's rounding on binary floats)."""
    if value is None:
        return None
    if isinstance(value, Decimal128):
        value = value.to_decimal()
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return float(value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: (punch_out - punch_in) seconds / 3600, 2 decimals half-up."""
    seconds = (punch_out - punch_in) // timedelta(seconds=1)
    return round_half_up(Decimal(seconds) / Decimal(3600), 2)


def derived_fields(status: str, punch_in, punch_out, emp: dict, day: date) -> dict:
    """The four stored derived values for a record (DATA_MODEL.md section 3 table)."""
    if status not in PRESENCE_STATUSES or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = late_minutes(punch_in, emp, day)
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hours = work_hours(punch_in, punch_out)
    return {
        "work_hours": hours,
        "late_minutes": late,
        "overtime_minutes": overtime_minutes(punch_out, emp, day),
        "half_day": hours < HALF_DAY_HOURS,  # R5 uses the rounded value
    }


def count_weekdays(first: date, last: date) -> int:
    """Mon-Fri days in [first, last] (R7). Calendar arithmetic only; no data involved."""
    if first > last:
        return 0
    return sum(1 for i in range((last - first).days + 1) if (first + timedelta(days=i)).weekday() < 5)


def month_bounds(month: str) -> tuple[date, date]:
    year, mon = map(int, month.split("-"))
    first = date(year, mon, 1)
    nxt = date(year + (mon == 12), mon % 12 + 1, 1)
    return first, nxt - timedelta(days=1)


# --------------------------------------------------------------------------- #
# 3. Serialisation and validation helpers
# --------------------------------------------------------------------------- #
def _api_value(value):
    return dt_to_ms(value) if isinstance(value, datetime) else value


def history_out(entry: dict) -> dict:
    return {
        "at": dt_to_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        "changes": {
            field: {"from": _api_value(change.get("from")), "to": _api_value(change.get("to"))}
            for field, change in (entry.get("changes") or {}).items()
        },
    }


def record_out(doc: dict) -> dict:
    """Stored record -> API shape. Never exposes _id; fills defaults for legacy records."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in doc.get("history") or []],
    }


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def unprocessable(msg: str, loc: tuple) -> HTTPException:
    """A 422 in the same shape FastAPI uses for its own validation errors."""
    return HTTPException(status_code=422, detail=[{"type": "value_error", "loc": list(loc), "msg": msg}])


def parse_day(value: str, loc: tuple) -> date:
    if not DATE_RE.match(value or ""):
        raise unprocessable("must be a date in YYYY-MM-DD format", loc)
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise unprocessable("not a valid calendar date", loc)


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees_col.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4. Request models
# --------------------------------------------------------------------------- #
# strict=True: floats (even 1.0), numeric strings and booleans are rejected with 422.
EpochMs = Annotated[int, Field(strict=True, ge=MIN_EPOCH_MS, le=MAX_EPOCH_MS)]
HHMM = Annotated[str, Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]


class EmployeeIn(BaseModel):
    emp_code: Annotated[str, Field(pattern=r"^EMP\d{4,6}$")]
    name: Annotated[str, Field(min_length=1, max_length=100)]
    email: Annotated[str, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, Field(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, value: str) -> str:
        if not DATE_RE.match(value):
            raise ValueError("must be a date in YYYY-MM-DD format")
        date.fromisoformat(value)  # raises ValueError for e.g. 2026-02-30
        return value

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: EpochMs = Field(default_factory=now_ms)
    status: PresenceName = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: EpochMs = Field(default_factory=now_ms)


class RegularizeIn(BaseModel):
    # Derived fields (work_hours, ...) are not declared, so a client sending them is ignored.
    status: Optional[StatusName] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: Annotated[str, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[str, Field(min_length=1, max_length=50)]

    @model_validator(mode="before")
    @classmethod
    def _no_explicit_nulls(cls, data):
        # "Omitted" means unchanged; an explicit null is not a valid value for these fields.
        if isinstance(data, dict):
            for field in ("status", "punch_in", "punch_out"):
                if field in data and data[field] is None:
                    raise ValueError(f"{field} may be omitted but not null")
        return data


# --------------------------------------------------------------------------- #
# 5. Indexes (created at startup, idempotent: same name + same spec is a no-op)
# --------------------------------------------------------------------------- #
def ensure_indexes() -> None:
    # employees
    employees_col.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
    employees_col.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="dept_emp_code")
    employees_col.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="dept_joined_on")
    employees_col.create_index([("joined_on", ASCENDING)], name="joined_on")
    # attendance_logs
    logs_col.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_date")
    logs_col.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp_code")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # keep serving (/health will report the DB state) but make it visible
        log.error("index creation failed: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 6. Employee and attendance endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(status_code=503, detail="MongoDB is not reachable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = EPOCH + timedelta(milliseconds=now_ms())
    try:
        employees_col.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    query = {} if department is None else {"department": department}
    total = employees_col.count_documents(query)
    cursor = (
        employees_col.find(query, {"_id": 0})
        .sort("emp_code", ASCENDING)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    punched = ms_to_dt(body.punched_at)
    day = attendance_date(punched, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": punched,
        "punch_out": None,
        **derived_fields(body.status, punched, None, emp, day),
        "history": [],
    }
    try:
        logs_col.insert_one(doc)  # unique (emp_code, date): concurrent duplicates -> exactly one succeeds
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    punched = ms_to_dt(body.punched_at)
    # Most recent record whose punch_in is at or before punched_at. For one employee, date order equals
    # punch_in order, so the unique (emp_code, date) index serves this sort. Works across midnight.
    record = logs_col.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": punched}},
        sort=[("date", DESCENDING)],
    )
    if record is None:
        raise HTTPException(status_code=404, detail="no punch-in found")
    if record.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="already punched out")
    started = record["punch_in"]
    if punched <= started or punched - started > MAX_SHIFT:
        raise unprocessable("punched_at must be after punch_in and within 24 hours of it", ("body", "punched_at"))

    day = date.fromisoformat(record["date"])
    derived = derived_fields(record["status"], started, punched, emp, day)
    # Conditional update: only succeeds while punch_out is still null, so two simultaneous punch-outs
    # cannot both win (the loser gets 409).
    updated = logs_col.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {"$set": {"punch_out": punched, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="already punched out")
    return record_out(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[StatusName] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    query = attendance_query(emp_code, date_from, date_to, status)
    # No filter: collection metadata gives the count instantly instead of counting 100k documents.
    total = logs_col.count_documents(query) if query else logs_col.estimated_document_count()
    cursor = (
        logs_col.find(query)
        .sort(ATTENDANCE_SORT)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


def _same(old, new) -> bool:
    if isinstance(old, datetime) or isinstance(new, datetime):
        return dt_to_ms(old) == dt_to_ms(new)
    return old == new


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, body: RegularizeIn, date_str: str = Path(..., alias="date")):
    day = parse_day(date_str, ("path", "date"))
    emp = get_employee_or_404(emp_code)
    record = logs_col.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if record is None:
        raise HTTPException(status_code=404, detail="no attendance record for that date")

    new_status = body.status or record["status"]
    if new_status in ABSENCE_STATUSES:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(f"{new_status} records cannot have punch times", ("body",))
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else record.get("punch_in")
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else record.get("punch_out")
        if new_in is None:
            raise unprocessable("a presence status requires a punch_in", ("body", "punch_in"))
        if body.punch_in is not None and attendance_date(new_in, emp) != day:
            raise unprocessable("punch_in must stay on the record's attendance date", ("body", "punch_in"))
        if new_out is not None and (new_out <= new_in or new_out - new_in > MAX_SHIFT):
            raise unprocessable("punch_out must be after punch_in and within 24 hours", ("body", "punch_out"))

    old_values = {
        "status": record["status"],
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "work_hours": record.get("work_hours"),
        "late_minutes": record.get("late_minutes") or 0,
        "overtime_minutes": record.get("overtime_minutes") or 0,
        "half_day": bool(record.get("half_day") or False),
    }
    new_values = {
        "status": new_status,
        "punch_in": new_in,
        "punch_out": new_out,
        **derived_fields(new_status, new_in, new_out, emp, day),
    }
    changes = {
        field: {"from": old_values[field], "to": new_values[field]}
        for field in new_values
        if not _same(old_values[field], new_values[field])
    }
    if not changes:
        raise unprocessable("the request changes nothing", ("body",))

    entry = {
        "at": EPOCH + timedelta(milliseconds=now_ms()),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }
    # Optimistic concurrency: the update only applies if the record still looks exactly like what we read
    # (same punch times, status and number of history entries). A concurrent edit makes it match nothing,
    # and the loser gets 409 instead of silently overwriting or losing a history entry.
    guard = {
        "_id": record["_id"],
        "status": record["status"],
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(record.get("history") or [])]},
    }
    updated = logs_col.find_one_and_update(
        guard,
        {"$set": new_values, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="record was modified concurrently, retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 7. Query / pipeline builders (used by the endpoints AND by /admin/explain)
# --------------------------------------------------------------------------- #
ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


def attendance_query(emp_code, date_from, date_to, status) -> dict:
    query: dict = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    start = parse_day(date_from, ("query", "date_from")) if date_from is not None else None
    end = parse_day(date_to, ("query", "date_to")) if date_to is not None else None
    if start and end and start > end:
        raise unprocessable("date_from must be on or before date_to", ("query", "date_from"))
    if start or end:
        query["date"] = {}
        if start:
            query["date"]["$gte"] = start.isoformat()
        if end:
            query["date"]["$lte"] = end.isoformat()
    if status is not None:
        query["status"] = status
    return query


# Expression helpers. `p` is the path prefix: "$" for a top-level document, "$$l." inside $map/$filter.
def is_present(p: str) -> dict:
    return {"$in": [p + "status", PRESENCE_STATUSES]}


def is_weekday(p: str) -> dict:  # R7: Mon-Fri ($isoDayOfWeek: Mon=1 ... Sun=7)
    return {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": p + "date", "format": "%Y-%m-%d"}}}, 5]}


def present_value(p: str) -> dict:  # R5: half day counts 0.5; missing half_day means false
    return {"$cond": [{"$eq": [{"$ifNull": [p + "half_day", False]}, True]}, 0.5, 1]}


def late_value(p: str) -> dict:
    return {"$ifNull": [p + "late_minutes", 0]}


def has_work_hours(p: str) -> dict:
    return {"$and": [is_present(p), {"$ne": [{"$ifNull": [p + "work_hours", None]}, None]}]}


def round4_half_up(expr) -> dict:
    """Half-up rounding to 4 decimals inside MongoDB ($round rounds half to even). Works on Decimal128."""
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [expr, 10000]}, Decimal128("0.5")]}}, 10000]}


def employee_monthly_pipeline(emp_code: str, first: date, last: date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": {"$cond": [{"$and": [is_present("$"), is_weekday("$")]}, present_value("$"), 0]}},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [late_value("$"), 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": late_value("$")},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


def department_summary_pipeline(first: date, last: date, department: Optional[str]) -> list:
    """Starts from employees so that people with zero logs are still counted (R9)."""
    match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department

    def over_logs(expr_in) -> dict:  # apply an expression to every joined log and sum it
        return {"$sum": {"$map": {"input": "$logs", "as": "l", "in": expr_in}}}

    def count_logs(cond) -> dict:
        return {"$size": {"$filter": {"input": "$logs", "as": "l", "cond": cond}}}

    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",  # served by the unique (emp_code, date) index
            "pipeline": [
                {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
                {"$project": {"_id": 0, "status": 1, "date": 1, "half_day": 1, "work_hours": 1, "late_minutes": 1}},
            ],
            "as": "logs",
        }},
        {"$project": {
            "_id": 0,
            "department": 1,
            "present_days": over_logs(
                {"$cond": [{"$and": [is_present("$$l."), is_weekday("$$l.")]}, present_value("$$l."), 0]}),
            # exact decimal sum of work_hours, so the average is not skewed by float error
            "wh_sum": over_logs(
                {"$cond": [has_work_hours("$$l."), {"$toDecimal": "$$l.work_hours"}, Decimal128("0")]}),
            "wh_count": count_logs(has_work_hours("$$l.")),
            "late_count": count_logs({"$gt": [late_value("$$l."), 0]}),
            "total_late_minutes": over_logs(late_value("$$l.")),
            "leave_count": count_logs({"$eq": ["$$l.status", "LEAVE"]}),
            "on_duty_count": count_logs({"$eq": ["$$l.status", "ON_DUTY"]}),
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": "$present_days"},
            "wh_sum": {"$sum": "$wh_sum"},
            "wh_count": {"$sum": "$wh_count"},
            "late_count": {"$sum": "$late_count"},
            "total_late_minutes": {"$sum": "$total_late_minutes"},
            "leave_count": {"$sum": "$leave_count"},
            "on_duty_count": {"$sum": "$on_duty_count"},
        }},
        {"$sort": {"_id": 1}},
    ]


def late_leaderboard_pipeline(first: date, last: date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "pipeline": [{"$project": {"_id": 0, "name": 1, "department": 1}}],
            "as": "emp",
        }},
        {"$unwind": "$emp"},  # drops logs whose employee does not exist
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped (1, 2, 2, 4).
        # sortBy holds only the ranked value, so equal totals really tie.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking: ties at the cutoff are all kept
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "emp_code": "$_id",
            "name": "$emp.name",
            "department": "$emp.department",
            "total_late_minutes": 1,
            "late_count": 1,
        }},
    ]
    return pipeline


def department_trend_pipeline(department: str, emp_codes: list, first: date, last: date) -> list:
    start = datetime.combine(first, time(), UTC)
    end = datetime.combine(last, time(), UTC)
    return [
        {"$match": {"emp_code": {"$in": emp_codes}, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {
            "_id": "$date",
            "present_count": {"$sum": {"$cond": [is_present("$"), present_value("$"), 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [late_value("$"), 0]}, 1, 0]}},
        }},
        {"$project": {
            "_id": 0,
            "day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d", "timezone": "UTC"}},
            "present_count": 1,
            "late_count": 1,
        }},
        # Two anchor rows (first and last day) so the range is filled even when there are no records at all.
        {"$unionWith": {"pipeline": [{"$documents": [
            {"day": start, "present_count": 0, "late_count": 0},
            {"day": end, "present_count": 0, "late_count": 0},
        ]}]}},
        {"$group": {"_id": "$day", "present_count": {"$sum": "$present_count"}, "late_count": {"$sum": "$late_count"}}},
        {"$project": {"_id": 0, "day": "$_id", "present_count": 1, "late_count": 1}},
        # Gap filling: one row per calendar day (upper bound is exclusive).
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [start, end + timedelta(days=1)]}}},
        # R9 headcount for that day: department employees who had joined by then.
        {"$lookup": {
            "from": "employees",
            "let": {"d": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}}},
            "pipeline": [
                {"$match": {"department": department}},
                {"$match": {"$expr": {"$lte": ["$joined_on", "$$d"]}}},
                {"$count": "n"},
            ],
            "as": "hc",
        }},
        {"$set": {
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
            "headcount": {"$ifNull": [{"$first": "$hc.n"}, 0]},
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
        }},
        {"$set": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            round4_half_up({"$divide": [{"$toDecimal": "$present_count"}, {"$toDecimal": "$headcount"}]}),
            None,
        ]}}},
        # 7-row moving average inside the requested range only; $avg skips nulls and is null if all are null.
        {"$setWindowFields": {
            "sortBy": {"day": 1},
            "output": {"moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}},
        }},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
            "is_working_day": 1,
            "headcount": 1,
            "present_count": {"$toDouble": "$present_count"},
            "late_count": 1,
            "attendance_rate": {"$toDouble": "$attendance_rate"},
            "moving_avg_7d": {"$toDouble": round4_half_up("$moving_avg_7d")},
        }},
        {"$sort": {"date": 1}},
    ]


def trend_range(from_: str, to: str) -> tuple[date, date]:
    first = parse_day(from_, ("query", "from"))
    last = parse_day(to, ("query", "to"))
    if last < first:
        raise unprocessable("to must be on or after from", ("query", "to"))
    if (last - first).days + 1 > MAX_TREND_DAYS:
        raise unprocessable(f"the range may not exceed {MAX_TREND_DAYS} days", ("query", "to"))
    return first, last


def department_emp_codes(department: str) -> list:
    return [e["emp_code"] for e in employees_col.find({"department": department}, {"_id": 0, "emp_code": 1})]


def _int(value) -> int:
    return int(value or 0)


def _decimal(value) -> Decimal:
    return value.to_decimal() if isinstance(value, Decimal128) else Decimal(str(value or 0))


# --------------------------------------------------------------------------- #
# 8. Analytics endpoints (all heavy lifting happens in the pipelines above)
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=MONTH_PATTERN)):
    emp = get_employee_or_404(emp_code)
    first, last = month_bounds(month)
    joined = date.fromisoformat(emp["joined_on"])
    working_days = count_weekdays(max(first, joined), last)

    rows = list(logs_col.aggregate(employee_monthly_pipeline(emp_code, first, last)))
    row = rows[0] if rows else {}
    present = _decimal(row.get("present_days"))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": round_half_up(present, 2),
        "leave_days": _int(row.get("leave_days")),
        "late_count": _int(row.get("late_count")),
        "total_late_minutes": _int(row.get("total_late_minutes")),
        "total_overtime_minutes": _int(row.get("total_overtime_minutes")),
        "attendance_pct": round_half_up(present * 100 / working_days, 2) if working_days else None,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=MONTH_PATTERN), department: Optional[str] = None):
    first, last = month_bounds(month)
    items = []
    for row in employees_col.aggregate(department_summary_pipeline(first, last, department)):
        wh_count = _int(row["wh_count"])
        items.append({
            "department": row["_id"],
            "headcount": _int(row["headcount"]),
            "present_days": round_half_up(_decimal(row["present_days"]), 2),
            "avg_work_hours": round_half_up(_decimal(row["wh_sum"]) / wh_count, 2) if wh_count else None,
            "late_count": _int(row["late_count"]),
            "total_late_minutes": _int(row["total_late_minutes"]),
            "leave_count": _int(row["leave_count"]),
            "on_duty_count": _int(row["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    first, last = month_bounds(month)
    items = [
        {**row, "rank": _int(row["rank"]), "total_late_minutes": _int(row["total_late_minutes"]),
         "late_count": _int(row["late_count"])}
        for row in logs_col.aggregate(late_leaderboard_pipeline(first, last, limit, department))
    ]
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
):
    first, last = trend_range(from_, to)
    emp_codes = department_emp_codes(department)
    if not emp_codes:
        raise HTTPException(status_code=404, detail="unknown department")
    items = list(logs_col.aggregate(department_trend_pipeline(department, emp_codes, first, last)))
    for row in items:
        row["headcount"] = _int(row["headcount"])
        row["late_count"] = _int(row["late_count"])
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 9. Explain: runs explain("executionStats") on exactly what the endpoint runs
# --------------------------------------------------------------------------- #
def _missing(endpoint: str, **params):
    for name, value in params.items():
        if value is None:
            raise unprocessable(f"'{name}' is required to explain {endpoint}", ("query", name))


def _explain_aggregate(collection: str, pipeline: list) -> dict:
    return db.command("explain", {"aggregate": collection, "pipeline": pipeline, "cursor": {}},
                      verbosity="executionStats")


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                      "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_PATTERN),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[StatusName] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    if endpoint == "attendance_list":
        collection = "attendance_logs"
        command = {
            "find": collection,
            "filter": attendance_query(emp_code, date_from, date_to, status),
            "sort": dict(ATTENDANCE_SORT),
            "skip": (page - 1) * page_size,
            "limit": page_size,
        }
        raw = db.command("explain", command, verbosity="executionStats")
    elif endpoint == "employee_monthly":
        _missing(endpoint, emp_code=emp_code, month=month)
        collection = "attendance_logs"
        first, last = month_bounds(month)
        raw = _explain_aggregate(collection, employee_monthly_pipeline(emp_code, first, last))
    elif endpoint == "department_summary":
        _missing(endpoint, month=month)
        collection = "employees"
        first, last = month_bounds(month)
        raw = _explain_aggregate(collection, department_summary_pipeline(first, last, department))
    elif endpoint == "late_leaderboard":
        _missing(endpoint, month=month)
        collection = "attendance_logs"
        first, last = month_bounds(month)
        raw = _explain_aggregate(collection, late_leaderboard_pipeline(first, last, limit, department))
    else:  # department_trend
        _missing(endpoint, department=department, **{"from": from_, "to": to})
        collection = "attendance_logs"
        first, last = trend_range(from_, to)
        pipeline = department_trend_pipeline(department, department_emp_codes(department), first, last)
        raw = _explain_aggregate(collection, pipeline)

    return {"endpoint": endpoint, "collection": collection, "explain": json.loads(json_util.dumps(raw))}
