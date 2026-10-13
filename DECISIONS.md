# DECISIONS.md

1. **Indexes.**
   - `employees`:
     - unique `emp_code`: makes duplicate creates impossible, and serves lookups.
     - `(department, emp_code)`: the filtered, sorted employee list.
     - `(department, joined_on)` and `joined_on`: headcount matching in the summary and trend.
   - `attendance_logs`:
     - unique `(emp_code, date)`: one record per day, per-employee ranges, punch-out.
     - `(date desc, emp_code)`: matches the list sort exactly and serves month ranges.

   I rejected an index on `status` (only five values, so it filters poorly) and one on `(emp_code, punch_in)`, because date order already equals punch-in order.

2. **Punch-in race.** Both requests compute the same `date` and call `insert_one` directly, with no "check first". The unique `(emp_code, date)` index lets exactly one insert succeed. The other raises `DuplicateKeyError`, which I return as 409.

3. **Ties.** I use `$rank` sorted only by total late minutes, so equal totals share a rank and the next rank is skipped. I keep every row with `rank <= limit`, so a tie at the cutoff returns both people, even beyond `limit`. Ties are ordered by `emp_code`.

4. **Headcount.** The pipeline starts from `employees` (filtered by `joined_on`), not from the logs. It then joins each person's logs with `$lookup`. Someone with no logs still has a row with an empty array, so they count.

5. **100x data.** I would store pre-aggregated daily per-department and per-employee summary documents, updated on every punch and correction. Analytics would then read a few hundred rows instead of scanning millions of logs.
