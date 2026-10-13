"""
End-to-end self-test against a real MongoDB (local or Atlas).

    python tests/selftest.py            # small awkward dataset: rules, races, analytics, explain
    python tests/selftest.py --bulk     # also loads ~100k records and checks explain + timings

It uses a separate database, <MONGO_DB>_selftest, which it drops before and after the run, so your
normal data is never touched. Analytics results are compared with expected values computed here
in plain Python (written independently of the pipelines in app/main.py).
"""
import json
import os
import random
import sys
import threading
import time as clock
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")
os.environ["MONGO_DB"] = os.environ["MONGO_DB"] + "_selftest"

import uvicorn  # noqa: E402
from app import main  # noqa: E402

PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
MONTH = "2026-07"
PRESENCE = {"PRESENT", "WFH", "ON_DUTY"}
failures: list[str] = []


# ------------------------------------------------------------------ helpers
def call(method: str, path: str, body=None, params=None):
    url = BASE + path
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read() or b"null")


def check(name: str, got, expected):
    if got == expected:
        print(f"  PASS  {name}", flush=True)
    else:
        failures.append(name)
        print(f"  FAIL  {name}\n        got:      {got}\n        expected: {expected}", flush=True)


def ms(y, mo, d, h, mi, s=0):
    return int(datetime(y, mo, d, h, mi, s, tzinfo=IST).timestamp()) * 1000


def bson_dt(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=IST).astimezone(UTC)


def q(value, places):
    return float(Decimal(value).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


# ------------------------------------------------------------------ dataset
EMPLOYEES = [
    # code, dept, shift, joined
    ("EMP0001", "Engineering", "09:30", "18:30", "2026-01-05"),
    ("EMP0002", "Engineering", "09:30", "18:30", "2026-01-05"),
    ("EMP0003", "Engineering", "09:30", "18:30", "2026-07-15"),  # mid-month joiner
    ("EMP0004", "Engineering", "09:30", "18:30", "2026-08-10"),  # joins after July: not in July headcount
    ("EMP0005", "Sales", "22:00", "06:00", "2026-02-01"),        # overnight shift
    ("EMP0006", "Sales", "09:00", "18:00", "2026-03-01"),        # never has any logs
    ("EMP0007", "Sales", "10:00", "19:00", "2026-01-01"),
    ("EMP0008", "Ops", "09:30", "18:30", "2026-05-01"),
    ("EMP0009", "engineering", "09:30", "18:30", "2026-01-01"),  # case-sensitive department
]


def build_logs():
    rng = random.Random(7)
    logs = []
    active = {"EMP0001": "2026-06-25", "EMP0002": "2026-06-25", "EMP0003": "2026-07-15", "EMP0005": "2026-06-25",
              "EMP0007": "2026-06-25", "EMP0008": "2026-06-25", "EMP0009": "2026-06-25"}
    day = date(2026, 6, 25)
    while day <= date(2026, 8, 5):
        ds = day.isoformat()
        for code, since in active.items():
            if ds < since:
                continue
            weekend = day.weekday() >= 5
            if weekend and rng.random() < 0.8:
                continue  # few weekend records
            if not weekend and rng.random() < 0.12:
                continue  # missing weekday: no record at all
            status = rng.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [60, 15, 8, 8, 9])[0]
            doc = {"emp_code": code, "date": ds, "status": status}
            if status in PRESENCE:
                late = rng.choice([0, 0, 0, 0, 10, 12, 15, 25, 35, 45])
                pin = bson_dt(day.year, day.month, day.day, 9, 30) + timedelta(minutes=late)
                if rng.random() < 0.07:  # open record
                    doc.update(punch_in=pin, punch_out=None, work_hours=None, late_minutes=late, overtime_minutes=0,
                               half_day=False)
                else:
                    wh = rng.choice([3.5, 4.25, 4.49, 4.5, 7.75, 8.0, 8.33, 9.12, 9.5, 10.07])
                    pout = pin + timedelta(minutes=int(wh * 60))
                    doc.update(punch_in=pin, punch_out=pout, work_hours=wh, late_minutes=late,
                               overtime_minutes=rng.choice([0, 0, 30, 45]), half_day=wh < 4.5)
            else:
                doc.update(punch_in=None, punch_out=None, work_hours=None, late_minutes=0, overtime_minutes=0,
                           half_day=False)
            if rng.random() < 0.15:  # legacy record: no history / half_day
                doc.pop("half_day", None)
            else:
                doc["history"] = []
            logs.append(doc)
        day += timedelta(days=1)

    # Logs of an employee that does not exist: must be ignored by leaderboard and summary.
    logs.append({"emp_code": "EMP9999", "date": "2026-07-08", "status": "PRESENT", "punch_in": bson_dt(2026, 7, 8, 11, 0),
                 "punch_out": bson_dt(2026, 7, 8, 19, 0), "work_hours": 8.0, "late_minutes": 90,
                 "overtime_minutes": 0, "half_day": False, "history": []})

    # Force ties on the late leaderboard: EMP0001 == EMP0002 at the top, EMP0005 == EMP0007 just below.
    def total(code):
        return sum(d.get("late_minutes", 0) for d in logs if d["emp_code"] == code and d["date"].startswith(MONTH))

    top = max(total(c) for c in active) + 50
    second = top - 20
    for code, target, ds in [("EMP0001", top, "2026-07-04"), ("EMP0002", top, "2026-07-11"),
                             ("EMP0005", second, "2026-07-18"), ("EMP0007", second, "2026-07-25")]:
        logs = [d for d in logs if not (d["emp_code"] == code and d["date"] == ds)]  # weekend slot
        need = target - total(code)
        logs.append({"emp_code": code, "date": ds, "status": "PRESENT", "punch_in": bson_dt(2026, 7, 4, 12, 0),
                     "punch_out": bson_dt(2026, 7, 4, 20, 0), "work_hours": 8.0, "late_minutes": need,
                     "overtime_minutes": 0, "half_day": False, "history": []})
    return logs


# ------------------------------------------------------------------ expected values (plain Python)
def emp_map():
    return {e[0]: {"department": e[1], "joined_on": e[4], "name": f"Name {e[0]}"} for e in EMPLOYEES}


def in_month(doc, month=MONTH):
    return doc["date"].startswith(month)


def weekday(ds):
    return date.fromisoformat(ds).weekday() < 5


def present_value(doc):
    if doc["status"] not in PRESENCE:
        return Decimal(0)
    return Decimal("0.5") if doc.get("half_day") is True else Decimal(1)


def expected_monthly(logs, code):
    emp = emp_map()[code]
    first, last = date(2026, 7, 1), date(2026, 7, 31)
    start = max(first, date.fromisoformat(emp["joined_on"]))
    wd = sum(1 for i in range((last - start).days + 1) if (start + timedelta(days=i)).weekday() < 5) if start <= last else 0
    mine = [d for d in logs if d["emp_code"] == code and in_month(d)]
    present = sum((present_value(d) for d in mine if weekday(d["date"])), Decimal(0))
    return {
        "emp_code": code, "month": MONTH, "working_days": wd, "present_days": float(present),
        "leave_days": sum(d["status"] == "LEAVE" for d in mine),
        "late_count": sum(d.get("late_minutes", 0) > 0 for d in mine),
        "total_late_minutes": sum(d.get("late_minutes", 0) for d in mine),
        "total_overtime_minutes": sum(d.get("overtime_minutes", 0) for d in mine),
        "attendance_pct": q(present * 100 / wd, 2) if wd else None,
    }


def expected_summary(logs, department=None):
    emps = emp_map()
    out = {}
    for code, e in emps.items():
        if e["joined_on"] > "2026-07-31" or (department and e["department"] != department):
            continue
        row = out.setdefault(e["department"], {"headcount": 0, "present": Decimal(0), "wh": Decimal(0), "whn": 0,
                                               "late_count": 0, "late": 0, "leave": 0, "od": 0})
        row["headcount"] += 1
        for d in logs:
            if d["emp_code"] != code or not in_month(d):
                continue
            if weekday(d["date"]):
                row["present"] += present_value(d)
            if d["status"] in PRESENCE and d.get("work_hours") is not None:
                row["wh"] += Decimal(str(d["work_hours"]))
                row["whn"] += 1
            row["late_count"] += d.get("late_minutes", 0) > 0
            row["late"] += d.get("late_minutes", 0)
            row["leave"] += d["status"] == "LEAVE"
            row["od"] += d["status"] == "ON_DUTY"
    return {"month": MONTH, "items": [
        {"department": k, "headcount": v["headcount"], "present_days": float(v["present"]),
         "avg_work_hours": q(v["wh"] / v["whn"], 2) if v["whn"] else None, "late_count": v["late_count"],
         "total_late_minutes": v["late"], "leave_count": v["leave"], "on_duty_count": v["od"]}
        for k, v in sorted(out.items())]}


def expected_leaderboard(logs, limit, department=None):
    emps = emp_map()
    totals = {}
    for d in logs:
        if in_month(d) and d.get("late_minutes", 0) > 0 and d["emp_code"] in emps:
            if department and emps[d["emp_code"]]["department"] != department:
                continue
            t = totals.setdefault(d["emp_code"], [0, 0])
            t[0] += d["late_minutes"]
            t[1] += 1
    rows = sorted(totals.items(), key=lambda kv: (-kv[1][0], kv[0]))
    items = []
    for code, (tot, cnt) in rows:
        rank = 1 + sum(1 for _, (o, _) in rows if o > tot)
        if rank <= limit:
            items.append({"rank": rank, "emp_code": code, "name": emps[code]["name"],
                          "department": emps[code]["department"], "total_late_minutes": tot, "late_count": cnt})
    return {"month": MONTH, "items": items}


def expected_trend(logs, department, first, last):
    emps = {c: e for c, e in emp_map().items() if e["department"] == department}
    items, rates = [], []
    day = first
    while day <= last:
        ds = day.isoformat()
        todays = [d for d in logs if d["date"] == ds and d["emp_code"] in emps]
        present = sum((present_value(d) for d in todays), Decimal(0))
        headcount = sum(1 for e in emps.values() if e["joined_on"] <= ds)
        working = day.weekday() < 5
        rate = (present / headcount).quantize(Decimal("0.0001"), ROUND_HALF_UP) if working and headcount else None
        rates.append(rate)
        window = [r for r in rates[-7:] if r is not None]
        ma = (sum(window) / len(window)).quantize(Decimal("0.0001"), ROUND_HALF_UP) if window else None
        items.append({"date": ds, "is_working_day": working, "headcount": headcount, "present_count": float(present),
                      "late_count": sum(d.get("late_minutes", 0) > 0 for d in todays),
                      "attendance_rate": None if rate is None else float(rate),
                      "moving_avg_7d": None if ma is None else float(ma)})
        day += timedelta(days=1)
    return {"department": department, "items": items}


def by_key(items, keys):
    return [{k: row.get(k) for k in keys} for row in items]


# ------------------------------------------------------------------ runner
def start_server():
    server = uvicorn.Server(uvicorn.Config(main.app, host="127.0.0.1", port=PORT, log_level="warning"))
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(100):
        try:
            if call("GET", "/health")[0] == 200:
                return server
        except OSError:
            pass
        clock.sleep(0.2)
    raise SystemExit("server did not become ready")


def reset():
    # drop the two test collections (works with Atlas' default read/write user role)
    main.db.employees.drop()
    main.db.attendance_logs.drop()


def seed(logs):
    db = main.db
    reset()
    db.employees.insert_many([
        {"emp_code": c, "name": f"Name {c}", "email": f"{c.lower()}@example.com", "department": dept,
         "shift_start": s, "shift_end": e, "joined_on": j, "created_at": datetime(2026, 1, 1, tzinfo=UTC)}
        for c, dept, s, e, j in EMPLOYEES])
    db.attendance_logs.insert_many([dict(d) for d in logs])
    main.ensure_indexes()


def test_rules():
    print("\nRules and status codes")
    check("R2 09:40:00 -> 0", call("POST", "/attendance/punch-in", {"emp_code": "EMP0008", "punched_at": ms(2026, 9, 1, 9, 40, 0) + 999})[1]["late_minutes"], 0)
    check("R2 09:40:01 -> 10", call("POST", "/attendance/punch-in", {"emp_code": "EMP0008", "punched_at": ms(2026, 9, 2, 9, 40, 1)})[1]["late_minutes"], 10)
    check("R2 10:15:59 -> 45", call("POST", "/attendance/punch-in", {"emp_code": "EMP0008", "punched_at": ms(2026, 9, 3, 10, 15, 59)})[1]["late_minutes"], 45)
    status, rec = call("POST", "/attendance/punch-in", {"emp_code": "EMP0005", "punched_at": ms(2026, 9, 2, 1, 15)})
    check("R1 overnight 01:15 belongs to previous day", (status, rec["date"]), (201, "2026-09-01"))
    status, rec = call("POST", "/attendance/punch-out", {"emp_code": "EMP0005", "punched_at": ms(2026, 9, 2, 6, 45)})
    check("overnight punch-out across midnight", (status, rec["overtime_minutes"], rec["work_hours"]), (200, 45, 5.5))
    status, rec = call("POST", "/attendance/punch-out", {"emp_code": "EMP0008", "punched_at": ms(2026, 9, 1, 11, 47, 30)})
    check("R4 2h07m30s -> 2.13 and R5 half day", (status, rec["work_hours"], rec["half_day"]), (200, 2.13, True))
    check("seconds timestamp -> 422", call("POST", "/attendance/punch-in", {"emp_code": "EMP0008", "punched_at": 1788000000})[0], 422)
    check("float timestamp -> 422", call("POST", "/attendance/punch-in", {"emp_code": "EMP0008", "punched_at": 1788000000000.0})[0], 422)
    check("unknown employee -> 404", call("POST", "/attendance/punch-in", {"emp_code": "EMP4040"})[0], 404)


def test_races():
    print("\nConcurrency")
    body = {"emp_code": "EMP0007", "punched_at": ms(2026, 9, 7, 10, 0)}
    with ThreadPoolExecutor(12) as pool:
        codes = sorted(r[0] for r in pool.map(lambda _: call("POST", "/attendance/punch-in", body), range(12)))
    check("12 parallel punch-ins -> one 201, rest 409", (codes.count(201), codes.count(409)), (1, 11))
    body = {"emp_code": "EMP0007", "punched_at": ms(2026, 9, 7, 19, 0)}
    with ThreadPoolExecutor(12) as pool:
        codes = sorted(r[0] for r in pool.map(lambda _: call("POST", "/attendance/punch-out", body), range(12)))
    check("12 parallel punch-outs -> one 200, rest 409", (codes.count(200), codes.count(409)), (1, 11))

    def patch(i):
        return call("PATCH", "/attendance/EMP0007/2026-09-07",
                    {"punch_in": ms(2026, 9, 7, 9, 0) + i * 60_000, "reason": f"fix number {i}", "regularized_by": "hr"})
    with ThreadPoolExecutor(10) as pool:
        codes = [r[0] for r in pool.map(patch, range(1, 11))]
    history = call("GET", "/attendance", params={"emp_code": "EMP0007", "date_from": "2026-09-07", "date_to": "2026-09-07"})[1]["items"][0]["history"]
    check("parallel PATCH: every 200 has its history entry, no 500", (len(history) == codes.count(200), 500 in codes), (True, False))


def test_analytics(logs):
    print("\nAnalytics vs. independently computed expectations")
    for code in ["EMP0001", "EMP0003", "EMP0004", "EMP0005", "EMP0006", "EMP0009"]:
        check(f"monthly {code}", call("GET", f"/analytics/employees/{code}/monthly", params={"month": MONTH})[1],
              expected_monthly(logs, code))
    check("department summary (all)", call("GET", "/analytics/departments/summary", params={"month": MONTH})[1],
          expected_summary(logs))
    check("department summary (Sales only)", call("GET", "/analytics/departments/summary", params={"month": MONTH, "department": "Sales"})[1],
          expected_summary(logs, "Sales"))
    for limit in [1, 2, 3, 10]:
        check(f"leaderboard limit={limit}", call("GET", "/analytics/leaderboard/late", params={"month": MONTH, "limit": limit})[1],
              expected_leaderboard(logs, limit))
    check("leaderboard Sales only", call("GET", "/analytics/leaderboard/late", params={"month": MONTH, "limit": 1, "department": "Sales"})[1],
          expected_leaderboard(logs, 1, "Sales"))
    keys = ["date", "is_working_day", "headcount", "present_count", "late_count", "attendance_rate", "moving_avg_7d"]
    for dept, f, t in [("Engineering", date(2026, 7, 1), date(2026, 7, 31)), ("Sales", date(2026, 6, 20), date(2026, 8, 10)),
                       ("Ops", date(2026, 9, 20), date(2026, 9, 30))]:
        status, got = call("GET", f"/analytics/departments/{dept}/trend", params={"from": f, "to": t})
        exp = expected_trend(logs, dept, f, t)
        check(f"trend {dept} {f}..{t}", (status, got.get("department"), by_key(got.get("items", []), keys)),
              (200, exp["department"], exp["items"]))
    check("trend unknown department -> 404", call("GET", "/analytics/departments/Nope/trend", params={"from": "2026-07-01", "to": "2026-07-02"})[0], 404)


def test_explain():
    print("\nExplain plans (IXSCAN, no COLLSCAN)")
    cases = [
        ("attendance_list", {}),
        ("attendance_list", {"status": "LEAVE", "page": 2}),
        ("attendance_list", {"emp_code": "EMP0001", "date_from": "2026-07-01", "date_to": "2026-07-31"}),
        ("employee_monthly", {"emp_code": "EMP0001", "month": MONTH}),
        ("department_summary", {"month": MONTH}),
        ("department_summary", {"month": MONTH, "department": "Sales"}),
        ("late_leaderboard", {"month": MONTH}),
        ("late_leaderboard", {"month": MONTH, "department": "Sales"}),
        ("department_trend", {"department": "Engineering", "from": "2026-07-01", "to": "2026-07-31"}),
    ]
    for endpoint, params in cases:
        status, body = call("GET", f"/admin/explain/{endpoint}", params=params)
        text = json.dumps(body)
        check(f"explain {endpoint} {params}", (status, "IXSCAN" in text, "COLLSCAN" in text), (200, True, False))


def bulk_load(n=100_000):
    print(f"\nLoading ~{n} extra records (takes a minute on Atlas)...")
    db = main.db
    codes = [f"EMP{9000 + i:04d}" for i in range(400)]
    db.employees.insert_many([{"emp_code": c, "name": c, "email": f"{c}@x.com", "department": f"Dept{i % 12}",
                               "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2025-01-01",
                               "created_at": datetime(2025, 1, 1, tzinfo=UTC)} for i, c in enumerate(codes)])
    batch, start = [], date(2025, 1, 1)
    for i in range(n):
        day = start + timedelta(days=i // len(codes))
        batch.append({"emp_code": codes[i % len(codes)], "date": day.isoformat(), "status": "PRESENT",
                      "punch_in": datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=4),
                      "punch_out": None, "work_hours": None, "late_minutes": i % 40, "overtime_minutes": 0,
                      "half_day": False, "history": []})
        if len(batch) == 5000:
            db.attendance_logs.insert_many(batch)
            batch = []
    if batch:
        db.attendance_logs.insert_many(batch)
    for path, params in [("/attendance", {"status": "PRESENT", "page": 50}),
                         ("/analytics/departments/summary", {"month": "2025-06"}),
                         ("/analytics/leaderboard/late", {"month": "2025-06"}),
                         ("/analytics/departments/Dept3/trend", {"from": "2025-05-01", "to": "2025-07-31"})]:
        t0 = clock.perf_counter()
        status, _ = call("GET", path, params=params)
        print(f"  {status}  {path}  {1000 * (clock.perf_counter() - t0):.0f} ms")


def main_run():
    print(f"Using test database '{main.db.name}' (cloud databases are slow: allow 2-4 minutes)", flush=True)
    logs = build_logs()
    print(f"Seeding {len(logs)} records...", flush=True)
    seed(logs)
    print("Starting the app on port 8765...", flush=True)
    server = start_server()
    try:
        test_rules()
        test_races()
        test_analytics(logs)
        test_explain()
        if "--bulk" in sys.argv:
            bulk_load()
            test_explain()
    finally:
        server.should_exit = True
        reset()
    print("\n" + ("ALL CHECKS PASSED" if not failures else f"{len(failures)} FAILED: {failures}"))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main_run()
