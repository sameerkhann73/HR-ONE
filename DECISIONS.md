# DECISIONS.md

1. **Indexes.** Unique `emp_code` on employees and unique `(emp_code, date)` on attendance_logs: these are the duplicate guards. `(date desc, emp_code)` serves the list sort and the leaderboard's date range, `(status, date desc, emp_code)` the status filter, and `(emp_code, punch_in desc)` finds the latest record at punch-out. On employees, `(department, joined_on)` and `(joined_on)` serve headcount and trend. I rejected a partial index on `late_minutes > 0`: the date range already narrows the scan and it would slow writes.

2. **Punch-in race.** Both requests compute the date and call `insert_one` straight away, with no existence check first. The unique `(emp_code, date)` index lets one insert win; the other gets `DuplicateKeyError` and I return 409.

3. **Ties.** I rank with `$rank` first and then keep `rank <= limit`. Everyone tied at the cutoff shares that rank, so all of them are returned and the list can be longer than `limit`.

4. **Headcount.** The summary starts from `employees` (joined on or before month end) and joins logs per employee. Someone with no logs gets an empty array and is still counted by `$sum: 1`.

5. **At 100x.** I would keep per-employee monthly totals, updated on punch and correction, instead of aggregating raw logs, and use keyset pagination instead of `skip`.
