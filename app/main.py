"""
Employee Attendance & Analytics API (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is read, but real env vars win)

Everything lives in this one file, in this order:
  1. config + small helpers (time, rounding, validation)
  2. pydantic models
  3. business rules (R1-R5)
  4. startup / indexes
  5. employees + attendance endpoints
  6. pipeline builders + analytics endpoints
  7. explain endpoint
"""
import calendar
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure, PyMongoError

load_dotenv()  # does not override variables that are already set in the environment

log = logging.getLogger("attendance")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

# tz_aware=True so every datetime that comes back from Mongo is UTC-aware.
# (The default gives naive datetimes, which is easy to mix up with local time.)
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=5000, maxPoolSize=100)
db = client[MONGO_DB]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

MIN_MS = 100_000_000_000      # anything smaller looks like seconds -> rejected
MAX_MS = 4_102_444_800_000    # 2100-01-01

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"

# Fields a correction can touch (used for history.changes)
TRACKED = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")


# --------------------------------------------------------------------------- #
# 1. Helpers
# --------------------------------------------------------------------------- #
def invalid(loc: tuple, msg: str) -> RequestValidationError:
    """Build a 422 that looks like FastAPI's own validation errors."""
    return RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg}])


def ms_to_dt(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def dt_to_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return (value - EPOCH) // timedelta(milliseconds=1)


def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def half_up(value: Any, places: int) -> Decimal:
    """Round half-up (ties go away from zero). Python's round() and Mongo's $round
    are both banker's rounding, which is why this exists (R4, R8)."""
    if isinstance(value, float):
        # strip binary float noise first, e.g. 0.12344999999999 -> 0.12345
        value = Decimal(repr(round(value, 9)))
    elif not isinstance(value, Decimal):
        value = Decimal(value)
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def parse_day(value: str, loc: tuple) -> date:
    """YYYY-MM-DD that is also a real calendar date, otherwise 422."""
    try:
        if len(value) != 10:
            raise ValueError
        return date.fromisoformat(value)
    except ValueError:
        raise invalid(loc, f"'{value}' is not a valid YYYY-MM-DD date")


def month_bounds(month: str) -> tuple[str, str]:
    """First and last calendar day of a YYYY-MM month, as strings."""
    year, mon = int(month[:4]), int(month[5:7])
    if year < 1:
        raise invalid(("query", "month"), "year out of range")
    last = calendar.monthrange(year, mon)[1]
    return f"{year:04d}-{mon:02d}-01", f"{year:04d}-{mon:02d}-{last:02d}"


def working_days(first: str, last: str, joined_on: str) -> int:
    """Mon-Fri days in [first, last], counting from joined_on if that is later (R7)."""
    start = max(date.fromisoformat(first), date.fromisoformat(joined_on))
    end = date.fromisoformat(last)
    count, day = 0, start
    while day <= end:
        if day.weekday() < 5:
            count += 1
        day += timedelta(days=1)
    return count


# --------------------------------------------------------------------------- #
# 2. Models
# --------------------------------------------------------------------------- #
# strict=True so 1.5, "1783312500000" and true are rejected instead of coerced
EpochMs = Annotated[int, Field(strict=True, ge=MIN_MS, le=MAX_MS)]


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HHMM_RE)
    shift_end: str = Field(default="18:30", pattern=HHMM_RE)
    joined_on: str = Field(pattern=DATE_RE)

    @field_validator("joined_on")
    @classmethod
    def _real_date(cls, v: str) -> str:
        try:
            date.fromisoformat(v)
        except ValueError:
            raise ValueError("joined_on is not a real calendar date")
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_null(self):
        # omitting a field means "leave it alone"; sending null is not allowed
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} must not be null")
        return self


# --------------------------------------------------------------------------- #
# 3. Business rules
# --------------------------------------------------------------------------- #
def _hm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    """R1: IST calendar date of the punch-in; early-morning punches on an overnight
    shift belong to the shift that started the day before."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if shift_end <= shift_start:  # overnight ("HH:MM" strings are zero padded, so this compares fine)
        eh, em = _hm(shift_end)
        if (local.hour, local.minute, local.second) < (eh, em, 0):
            day -= timedelta(days=1)
    return day.isoformat()


def shift_window(day: str, shift_start: str, shift_end: str) -> tuple[datetime, datetime]:
    """Start and end of the shift that belongs to attendance date `day` (IST-aware)."""
    d = date.fromisoformat(day)
    sh, sm = _hm(shift_start)
    eh, em = _hm(shift_end)
    start = datetime(d.year, d.month, d.day, sh, sm, tzinfo=IST)
    end = datetime(d.year, d.month, d.day, eh, em, tzinfo=IST)
    if shift_end <= shift_start:
        end += timedelta(days=1)
    return start, end


def calc_late_minutes(punch_in: datetime, shift_start_dt: datetime) -> int:
    """R2: late only when strictly more than 10:00 after shift start; then floor minutes since start."""
    secs = (punch_in - shift_start_dt) // timedelta(seconds=1)
    return secs // 60 if secs > 600 else 0


def calc_overtime(punch_out: datetime, shift_end_dt: datetime) -> int:
    """R3: floor minutes after shift end, but only if that is at least 30."""
    secs = (punch_out - shift_end_dt) // timedelta(seconds=1)
    minutes = secs // 60
    return minutes if minutes >= 30 else 0


def calc_work_hours(punch_in: datetime, punch_out: datetime) -> Decimal:
    """R4: seconds / 3600, 2 decimals half-up (done on Decimals, not floats)."""
    secs = (punch_out - punch_in) // timedelta(seconds=1)
    return half_up(Decimal(secs) / Decimal(3600), 2)


def derive(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], emp: dict, day: str) -> dict:
    """All four stored derived fields for a record, following R2-R5 and the table in DATA_MODEL.md."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return out
    start, end = shift_window(day, emp["shift_start"], emp["shift_end"])
    out["late_minutes"] = calc_late_minutes(punch_in, start)
    if punch_out is not None:
        wh = calc_work_hours(punch_in, punch_out)
        out["work_hours"] = float(wh)
        out["overtime_minutes"] = calc_overtime(punch_out, end)
        out["half_day"] = wh < Decimal("4.50")  # compare the *rounded* value (R5)
    return out


# --- shaping documents for responses --------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def record_out(doc: dict) -> dict:
    history = []
    for h in doc.get("history") or []:
        changes = {}
        for field, change in (h.get("changes") or {}).items():
            if field in ("punch_in", "punch_out"):  # BSON dates inside a nested object
                change = {"from": dt_to_ms(change.get("from")), "to": dt_to_ms(change.get("to"))}
            changes[field] = change
        history.append({"at": dt_to_ms(h.get("at")), "by": h.get("by"), "reason": h.get("reason"), "changes": changes})
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": history,
    }


def get_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4. App + startup (indexes)
# --------------------------------------------------------------------------- #
indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent: create_index with the same spec is a no-op if the index exists."""
    global indexes_ready
    specs = [
        (db.employees, [("emp_code", ASCENDING)], {"unique": True, "name": "uq_emp_code"}),
        (db.employees, [("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "dept_emp_code"}),
        (db.employees, [("department", ASCENDING), ("joined_on", ASCENDING)], {"name": "dept_joined"}),
        (db.employees, [("joined_on", ASCENDING)], {"name": "joined"}),
        (db.attendance_logs, [("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True, "name": "uq_emp_date"}),
        (db.attendance_logs, [("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_emp"}),
        (db.attendance_logs, [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp"}),
        (db.attendance_logs, [("emp_code", ASCENDING), ("punch_in", DESCENDING)], {"name": "emp_punch_in"}),
    ]
    ok = True
    for coll, keys, opts in specs:
        try:
            coll.create_index(keys, **opts)
        except OperationFailure as exc:  # e.g. a unique index over dirty data; don't take the whole app down
            ok = False
            log.error("could not create index %s: %s", opts["name"], exc)
    indexes_ready = ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    deadline = time.monotonic() + 12
    while True:
        try:
            ensure_indexes()
            break
        except PyMongoError as exc:  # mongo not reachable yet
            log.warning("startup: mongo not ready (%s)", exc)
            if time.monotonic() > deadline:
                break
            time.sleep(1)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_down(_: Request, exc: PyMongoError):
    log.error("mongo error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database error"})


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    if not indexes_ready:
        try:
            ensure_indexes()
        except PyMongoError:
            pass
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# 5. Employees + attendance
# --------------------------------------------------------------------------- #
PageQ = Query(1, ge=1)
PageSizeQ = Query(20, ge=1, le=100)


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        # the unique index is the real guard; no find-then-insert
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: int = PageQ, page_size: int = PageSizeQ):
    q: dict = {} if department is None else {"department": department}
    total = db.employees.count_documents(q) if q else db.employees.estimated_document_count()
    skip = (page - 1) * page_size
    items = []
    if skip < total:
        cur = db.employees.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip(skip).limit(page_size)
        items = [employee_out(d) for d in cur]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    ts = ts.replace(microsecond=0)  # R1: whole seconds, and the truncated value is what we keep

    day = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": day,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive(body.status, ts, None, emp, day),
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date) index decides who wins a race
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    ts = (ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()).replace(microsecond=0)

    # latest record that started at or before the punch-out (works across midnight)
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}},
        sort=[("punch_in", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found for this employee")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")

    gap = ts - rec["punch_in"]
    if gap <= timedelta(0):
        raise invalid(("body", "punched_at"), "punched_at must be after the punch-in")
    if gap > timedelta(hours=24):
        raise invalid(("body", "punched_at"), "punched_at is more than 24 hours after the punch-in")

    fields = derive(rec["status"], rec["punch_in"], ts, emp, rec["date"])
    # Only update if the record is still open and unchanged since we read it. If two
    # punch-outs race, exactly one of them matches this filter.
    updated = db.attendance_logs.find_one_and_update(
        {"emp_code": rec["emp_code"], "date": rec["date"], "punch_in": rec["punch_in"], "punch_out": None},
        {"$set": {"punch_out": ts, **fields}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    # `date` has to keep this name to match the route; it only shadows the class inside this function
    day = date
    parse_day(day, ("path", "date"))
    emp = get_employee(emp_code)
    rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": day})
    if rec is None:
        raise HTTPException(404, "no attendance record for that date")

    new_status = body.status or rec["status"]
    given_in = ms_to_dt(body.punch_in).replace(microsecond=0) if body.punch_in is not None else None
    given_out = ms_to_dt(body.punch_out).replace(microsecond=0) if body.punch_out is not None else None

    if new_status in PRESENCE:
        new_in = given_in or rec.get("punch_in")
        new_out = given_out or rec.get("punch_out")
        if new_in is None:
            raise invalid(("body", "punch_in"), "a presence status needs a punch_in")
        if attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != day:
            raise invalid(("body", "punch_in"), "punch_in must stay on the record's attendance date")
        if new_out is not None and not timedelta(0) < new_out - new_in <= timedelta(hours=24):
            raise invalid(("body", "punch_out"), "punch_out must be after punch_in and within 24 hours")
    else:  # ABSENT / LEAVE wipe both punch times
        if given_in is not None or given_out is not None:
            raise invalid(("body",), f"{new_status} cannot be combined with punch times")
        new_in = new_out = None

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derive(new_status, new_in, new_out, emp, day)}
    old = {
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }
    changes = {f: {"from": old[f], "to": new[f]} for f in TRACKED if old[f] != new[f]}
    if not changes:
        raise invalid(("body",), "request does not change anything")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: the write only lands if the record is exactly as we read it
    # (same times/status and same number of history entries). Otherwise somebody else got
    # there first and we answer 409 instead of overwriting their history.
    seen_history = len(rec.get("history") or [])
    updated = db.attendance_logs.find_one_and_update(
        {
            "emp_code": emp_code,
            "date": day,
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, seen_history]},
        },
        {"$set": new, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "record was changed by someone else, please retry")
    return record_out(updated)


# shared by GET /attendance and its explain
ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


def attendance_filter(emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str],
                      status: Optional[str]) -> dict:
    q: dict = {}
    d_from = parse_day(date_from, ("query", "date_from")) if date_from is not None else None
    d_to = parse_day(date_to, ("query", "date_to")) if date_to is not None else None
    if d_from and d_to and d_from > d_to:
        raise invalid(("query", "date_from"), "date_from must not be after date_to")
    if emp_code is not None:
        q["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        q["date"] = {}
        if date_from is not None:
            q["date"]["$gte"] = date_from
        if date_to is not None:
            q["date"]["$lte"] = date_to
    if status is not None:
        q["status"] = status
    return q


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = Query(None, pattern=DATE_RE),
    date_to: Optional[str] = Query(None, pattern=DATE_RE),
    status: Optional[Status] = None,
    page: int = PageQ,
    page_size: int = PageSizeQ,
):
    q = attendance_filter(emp_code, date_from, date_to, status)
    total = db.attendance_logs.count_documents(q) if q else db.attendance_logs.estimated_document_count()
    skip = (page - 1) * page_size
    items = []
    if skip < total:
        cur = db.attendance_logs.find(q).sort(ATTENDANCE_SORT).skip(skip).limit(page_size)
        items = [record_out(d) for d in cur]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 6. Analytics: pipeline builders (shared with the explain endpoint) + endpoints
# --------------------------------------------------------------------------- #
# Expression fragments. Legacy documents may lack late_minutes / half_day, so default them.
IS_PRESENT = {"$in": ["$status", list(PRESENCE)]}
LATE = {"$ifNull": ["$late_minutes", 0]}
OVERTIME = {"$ifNull": ["$overtime_minutes", 0]}
HALF = {"$ifNull": ["$half_day", False]}
# `date` is an IST calendar date string; parsing it as UTC midnight keeps the weekday right.
# $dayOfWeek: 1 = Sunday ... 7 = Saturday, so Mon-Fri is 2..6.
IS_WEEKDAY = {"$in": [
    {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "onError": None, "onNull": None}}},
    [2, 3, 4, 5, 6],
]}
DAY_VALUE = {"$cond": [IS_PRESENT, {"$cond": [HALF, 0.5, 1]}, 0]}  # 1 / 0.5 / 0 for one record (R5, R6)


def monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first, "$lte": last}}},
        {"$group": {
            "_id": None,
            # present_days only counts Mon-Fri; late/overtime/leave count every record (R7)
            "present_days": {"$sum": {"$cond": [IS_WEEKDAY, DAY_VALUE, 0]}},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [LATE, 0]}, 1, 0]}},
            "total_late_minutes": {"$sum": LATE},
            "total_overtime_minutes": {"$sum": OVERTIME},
        }},
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Starts from `employees` so people with no logs still count towards headcount (R9)."""
    first, last = month_bounds(month)
    match: dict = {"joined_on": {"$lte": last}}
    if department is not None:
        match["department"] = department
    has_hours = {"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": first, "$lte": last}}},
                # collapse to one small doc per employee before joining back
                {"$group": {
                    "_id": None,
                    "present_days": {"$sum": {"$cond": [IS_WEEKDAY, DAY_VALUE, 0]}},
                    # work_hours summed as whole hundredths so the later average is exact
                    "wh_cents": {"$sum": {"$cond": [has_hours, {"$round": [{"$multiply": ["$work_hours", 100]}, 0]}, 0]}},
                    "wh_n": {"$sum": {"$cond": [has_hours, 1, 0]}},
                    "late_count": {"$sum": {"$cond": [{"$gt": [LATE, 0]}, 1, 0]}},
                    "total_late_minutes": {"$sum": LATE},
                    "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
                    "on_duty_count": {"$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}},
                }},
            ],
            "as": "agg",
        }},
        {"$set": {"agg": {"$first": "$agg"}}},  # missing when the employee has no logs
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": {"$ifNull": ["$agg.present_days", 0]}},
            "wh_cents": {"$sum": {"$ifNull": ["$agg.wh_cents", 0]}},
            "wh_n": {"$sum": {"$ifNull": ["$agg.wh_n", 0]}},
            "late_count": {"$sum": {"$ifNull": ["$agg.late_count", 0]}},
            "total_late_minutes": {"$sum": {"$ifNull": ["$agg.total_late_minutes", 0]}},
            "leave_count": {"$sum": {"$ifNull": ["$agg.leave_count", 0]}},
            "on_duty_count": {"$sum": {"$ifNull": ["$agg.on_duty_count", 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]


def leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = month_bounds(month)
    pipe: list = [
        {"$match": {"date": {"$gte": first, "$lte": last}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees disappear here
    ]
    if department is not None:
        pipe.append({"$match": {"emp.department": department}})  # rank inside the department only
    pipe += [
        # $rank = standard competition ranking (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit is applied on the rank, not on row count
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name", "department": "$emp.department",
            "total_late_minutes": 1, "late_count": 1,
        }},
    ]
    return pipe


def trend_pipeline(department: str, d_from: date, d_to: date) -> list:
    """One pass over the department's employees. Each employee contributes
       - one 'joined' event (clamped to the start of the range) and
       - one event per attendance record in the range.
    Events are summed per day, $densify adds the missing days, and window stages do the
    running headcount and the 7-row moving average."""
    s_from, s_to = d_from.isoformat(), d_to.isoformat()
    lo = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    hi = datetime(d_to.year, d_to.month, d_to.day, tzinfo=UTC)
    hi_exclusive = hi + timedelta(days=1)  # $densify bounds are [lower, upper)

    return [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": s_from, "$lte": s_to}}},
                {"$project": {"_id": 0, "d": "$date", "present": DAY_VALUE,
                              "late": {"$cond": [{"$gt": [LATE, 0]}, 1, 0]}}},
            ],
            "as": "logs",
        }},
        {"$project": {"_id": 0, "events": {"$concatArrays": [
            [{"d": {"$cond": [{"$lt": ["$joined_on", s_from]}, s_from, "$joined_on"]}, "joined": 1}],
            "$logs",
        ]}}},
        {"$unwind": "$events"},
        {"$replaceRoot": {"newRoot": "$events"}},
        {"$match": {"d": {"$lte": s_to}}},  # people who join after the range never count
        {"$group": {
            "_id": "$d",
            "present_count": {"$sum": "$present"},
            "late_count": {"$sum": "$late"},
            "joined": {"$sum": "$joined"},
        }},
        {"$set": {"day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        {"$sort": {"day": 1}},
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [lo, hi_exclusive]}}},
        {"$match": {"day": {"$lte": hi}}},  # belt and braces in case the upper bound is inclusive
        {"$set": {
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
            "joined": {"$ifNull": ["$joined", 0]},
        }},
        {"$setWindowFields": {
            "sortBy": {"day": 1},
            "output": {"headcount": {"$sum": "$joined", "window": {"documents": ["unbounded", "current"]}}},
        }},
        {"$set": {"is_working_day": {"$in": [{"$dayOfWeek": "$day"}, [2, 3, 4, 5, 6]]}}},
        {"$set": {
            # rate in ten-thousandths, rounded half-up with integer maths:
            # floor(p/h * 10000 + 0.5) == floor((p * 20000 + h) / (2h))
            "rate_k": {"$cond": [
                {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                {"$floor": {"$divide": [
                    {"$add": [{"$multiply": ["$present_count", 20000]}, "$headcount"]},
                    {"$multiply": [2, "$headcount"]},
                ]}},
                None,
            ]},
        }},
        {"$set": {"has_rate": {"$cond": [{"$eq": ["$rate_k", None]}, 0, 1]}}},
        {"$setWindowFields": {
            "sortBy": {"day": 1},
            "output": {
                "win_sum": {"$sum": "$rate_k", "window": {"documents": [-6, 0]}},
                "win_n": {"$sum": "$has_rate", "window": {"documents": [-6, 0]}},
            },
        }},
        {"$set": {
            "attendance_rate": {"$cond": [{"$eq": ["$rate_k", None]}, None, {"$divide": ["$rate_k", 10000]}]},
            # mean of the (already rounded) rates in the window, again rounded half-up to 4 places
            "moving_avg_7d": {"$cond": [
                {"$eq": ["$win_n", 0]},
                None,
                {"$divide": [
                    {"$floor": {"$divide": [{"$add": [{"$multiply": ["$win_sum", 2]}, "$win_n"]},
                                            {"$multiply": [2, "$win_n"]}]}},
                    10000,
                ]},
            ]},
        }},
        {"$sort": {"day": 1}},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
            "is_working_day": 1, "headcount": 1, "present_count": 1, "late_count": 1,
            "attendance_rate": 1, "moving_avg_7d": 1,
        }},
    ]


def check_trend_range(frm: str, to: str) -> tuple[date, date]:
    d_from = parse_day(frm, ("query", "from"))
    d_to = parse_day(to, ("query", "to"))
    if d_to < d_from:
        raise invalid(("query", "to"), "'to' must not be before 'from'")
    if (d_to - d_from).days + 1 > 92:
        raise invalid(("query", "to"), "range is longer than 92 days")
    return d_from, d_to


def _num(value: Any, places: int = 2) -> float:
    return float(half_up(value, places))


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(pattern=MONTH_RE)):
    first, last = month_bounds(month)
    emp = get_employee(emp_code)
    rows = list(db.attendance_logs.aggregate(monthly_pipeline(emp_code, month)))
    g = rows[0] if rows else {}

    days = working_days(first, last, emp["joined_on"])
    present = g.get("present_days", 0)
    pct = None
    if days > 0:
        pct = _num(Decimal(repr(float(present))) * 100 / days, 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": days,
        "present_days": _num(present, 2),
        "leave_days": int(g.get("leave_days", 0)),
        "late_count": int(g.get("late_count", 0)),
        "total_late_minutes": int(g.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(g.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(pattern=MONTH_RE), department: Optional[str] = None):
    items = []
    for g in db.employees.aggregate(department_summary_pipeline(month, department)):
        n = int(g["wh_n"])
        avg = _num(Decimal(int(g["wh_cents"])) / (100 * n), 2) if n else None  # mean over records, not per employee
        items.append({
            "department": g["_id"],
            "headcount": int(g["headcount"]),
            "present_days": _num(g["present_days"], 2),
            "avg_work_hours": avg,
            "late_count": int(g["late_count"]),
            "total_late_minutes": int(g["total_late_minutes"]),
            "leave_count": int(g["leave_count"]),
            "on_duty_count": int(g["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: str = Query(pattern=MONTH_RE), limit: int = Query(10, ge=1, le=50),
                     department: Optional[str] = None):
    rows = db.attendance_logs.aggregate(leaderboard_pipeline(month, limit, department))
    items = [{
        "rank": int(r["rank"]),
        "emp_code": r["emp_code"],
        "name": r["name"],
        "department": r["department"],
        "total_late_minutes": int(r["total_late_minutes"]),
        "late_count": int(r["late_count"]),
    } for r in rows]
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, frm: str = Query(alias="from", pattern=DATE_RE),
                     to: str = Query(pattern=DATE_RE)):
    d_from, d_to = check_trend_range(frm, to)
    if db.employees.count_documents({"department": department}, limit=1) == 0:
        raise HTTPException(404, "unknown department")

    items = []
    for r in db.employees.aggregate(trend_pipeline(department, d_from, d_to)):
        items.append({
            "date": r["date"],
            "is_working_day": bool(r["is_working_day"]),
            "headcount": int(r["headcount"]),
            "present_count": _num(r["present_count"], 2),
            "late_count": int(r["late_count"]),
            "attendance_rate": None if r.get("attendance_rate") is None else _num(r["attendance_rate"], 4),
            "moving_avg_7d": None if r.get("moving_avg_7d") is None else _num(r["moving_avg_7d"], 4),
        })
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 7. Explain
# --------------------------------------------------------------------------- #
def _explain(collection: str, *, pipeline: Optional[list] = None, find: Optional[dict] = None) -> dict:
    if pipeline is not None:
        inner = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}
    else:
        inner = {"find": collection, **find}
    raw = db.command({"explain": inner, "verbosity": "executionStats"})
    # explain contains Timestamps etc.; round-trip through extended JSON to make it serialisable
    clean = json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"collection": collection, "explain": clean}


def _require(**params):
    for name, value in params.items():
        if value is None:
            shown = {"frm": "from"}.get(name, name)
            raise invalid(("query", shown), f"{shown} is required for this endpoint")


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_RE),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = Query(None, pattern=DATE_RE),
    date_to: Optional[str] = Query(None, pattern=DATE_RE),
    status: Optional[Status] = None,
    frm: Optional[str] = Query(None, alias="from", pattern=DATE_RE),
    to: Optional[str] = Query(None, pattern=DATE_RE),
    page: int = PageQ,
    page_size: int = PageSizeQ,
):
    # each branch builds the query with the same helpers the real endpoint uses
    if endpoint == "attendance_list":
        q = attendance_filter(emp_code, date_from, date_to, status)
        spec = {"filter": q, "sort": dict(ATTENDANCE_SORT), "skip": (page - 1) * page_size, "limit": page_size}
        out = _explain("attendance_logs", find=spec)
    elif endpoint == "employee_monthly":
        _require(emp_code=emp_code, month=month)
        out = _explain("attendance_logs", pipeline=monthly_pipeline(emp_code, month))
    elif endpoint == "department_summary":
        _require(month=month)
        out = _explain("employees", pipeline=department_summary_pipeline(month, department))
    elif endpoint == "late_leaderboard":
        _require(month=month)
        out = _explain("attendance_logs", pipeline=leaderboard_pipeline(month, limit, department))
    else:  # department_trend
        _require(department=department, frm=frm, to=to)
        d_from, d_to = check_trend_range(frm, to)
        out = _explain("employees", pipeline=trend_pipeline(department, d_from, d_to))
    return {"endpoint": endpoint, **out}
