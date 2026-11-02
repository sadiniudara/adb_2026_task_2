Results

All runs on one machine (PostgreSQL, JDBC, isolation level READ COMMITTED).
Times vary from run to run, because deadlocks depend on timing.

Light load: 30 tasks, 3 queries per task, 4 threads

| Run | RestartBlocking time (ms) | RestartBlocking retries | NoLiveLocks time (ms) | NoLiveLocks restarts | NoLiveLocks max per task |
|-----|--------------------------:|------------------------:|----------------------:|---------------------:|-------------------------:|
| 1 | 10818 | 12 | 3843 | 0 | 0 |
| 2 | 19470 | 9 | 6479 | 0 | 0 |
| 3 | 4485 | 3 | 8582 | 5 | 2 |
| **Average** | **11591** | **8** | **6301** | **1.7** | **max 2** |

All runs finished 30 of 30 tasks.

One further RestartBlocking run: 13454 ms, 9 retries, one task restarted 4 times.

Part 4: starvation check, heavy load: 60 tasks, 5 queries, 8 threads

| Program | Time (ms) | Restarts | Most restarts of one task | Finished |
|---------|----------:|---------:|--------------------------:|----------|
| RestartBlocking (1 run) | 1399595 | 725 | 96 | 60 of 60 |
| NoLiveLocks (run 1) | 11526 | 3 | 1 | 60 of 60 |
| NoLiveLocks (run 2) | 11187 | 3 | 1 | 60 of 60 |


Observations
With the light load we saw no real starvation (the highest number we recorded for one task was 4 restarts).
With the heavy load, plain restart needed 96 restarts for one task and about 23 minutes. NoLiveLocks needed at most 1 restart per task and about 11 seconds.
NoLiveLocks still had aborts in one light-load run (5), so deadlocks are rare, not impossible.
The "run alone" rule was never triggered in the runs above (exclusive runs: 0); it is the safety net against starvation.
RestartBlocking heavy load was measured once (it takes about 23 minutes).