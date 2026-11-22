import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Homework 3 - Homework 2 revisited.
 *
 * Continuous closed-loop transaction stream with realistic workload distribution:
 *  - p < 0.6        POINT_READ: reads 3 distinct tuples (one SELECT each) and prints sum.
 *  - 0.6 <= p < 0.7 INSERT: inserts one new tuple (cleaned up after benchmark).
 *  - 0.7 <= p < 0.8 FULL_SCAN: calculates sum(data_value) across the table and prints sum.
 *  - p >= 0.8       WRITE: updates 3 distinct tuples in exact generated order (no sorting).
 *
 * Deadlocks (40P01 / 40001) are rolled back and retried with randomized exponential backoff.
 * Starvation is prevented via a fair ReentrantReadWriteLock admission gate after max_shared_attempts.
 *
 * Usage:
 *   java [-Dhot=N] [-DdeadlockTimeoutMs=N] NoLiveLocks [mode] [seconds] [threads] [optimized] [quiet]
 *   - mode:      serial | concurrent | scale | ablation | fixed (default: concurrent)
 *   - seconds:   duration in seconds (default: 30); in "fixed" mode the number of transactions
 *   - -Dhot=N:   optional, WRITEs only touch the first N tuples (provokes deadlocks)
 *   - threads:   worker count for concurrent mode (default: 4)
 *   - optimized: true | false (default: false)
 *   - quiet:     true | false (default: false)
 */
public class NoLiveLocks {

    // ---------------------------------------------------------------
    // Configuration
    // ---------------------------------------------------------------

    public static final int NUM_THREADS = 4;

    public static final int num_tuple = 50000;
    public static final double max_value = 50.0;
    public static final int num_queries_per_task = 3;

    public static final int max_shared_attempts = 3;

    static final long SEED = 123456L;

    /**
     * Optional hot-spot mode: java -Dhot=20 NoLiveLocks ...
     * WRITE transactions then pick their 3 tuples only from the first N ids,
     * so writers collide often and the deadlock handling (retry, back-off,
     * exclusive gate) can be shown. 0 (default) = all 50,000 tuples.
     */
    static final int HOT = Integer.getInteger("hot", 0);

    /**
     * Optional: java -DdeadlockTimeoutMs=50 NoLiveLocks ...
     * PostgreSQL waits deadlock_timeout (default 1 s) before it even looks for
     * a deadlock, so a deadlocked transaction stalls for a second. 0 = default.
     */
    static final int DEADLOCK_TIMEOUT_MS = Integer.getInteger("deadlockTimeoutMs", 0);

    enum Type {
        POINT_READ,
        INSERT,
        FULL_SCAN,
        WRITE
    }

    /**
     * Part 2: each bottleneck fix can be switched on separately, so its effect
     * can be measured on its own (see "ablation" mode).
     */
    enum Fix {
        SYNC_COMMIT_OFF,  // commit does not wait for the WAL flush to disk
        PARALLEL,         // max_parallel_workers_per_gather = 4 for the full scan
        FILLFACTOR,       // fillfactor 70: room for in-place (HOT) updates
        BATCH_UPDATES,    // the 3 updates sent as one JDBC batch, same order
        BUFFERED_OUTPUT   // console output buffered instead of one write per line
    }

    /** The fixes that the original "optimized" flag switched on. */
    static final EnumSet<Fix> OPTIMIZED_FIXES =
            EnumSet.of(Fix.SYNC_COMMIT_OFF, Fix.PARALLEL, Fix.FILLFACTOR);

    final boolean persistent;   // one connection per worker instead of per transaction
    final EnumSet<Fix> fixes;
    final PrintStream out;
    final boolean quiet;
    final int seconds;
    volatile long deadlineNanos;

    /**
     * Part 2: optional FINITE stream. If set, the transactions are generated
     * up front from one seed and handed out to the workers, so serial and
     * concurrent execution process exactly the same transactions and the
     * total time can be compared 1:1. null = continuous, time-based stream.
     */
    java.util.List<Blocker> fixedStream = null;
    final AtomicInteger fixedNext = new AtomicInteger(0);

    // ---------------------------------------------------------------
    // Statistics
    // ---------------------------------------------------------------

    final AtomicInteger txIds = new AtomicInteger(0);
    final AtomicInteger nextInsertId = new AtomicInteger(num_tuple);

    final AtomicInteger[] committed =
            new AtomicInteger[Type.values().length];

    final AtomicInteger totalRestarts = new AtomicInteger(0);
    final AtomicInteger maxRestarts = new AtomicInteger(0);
    final AtomicInteger exclusiveRuns = new AtomicInteger(0);
    final AtomicInteger failed = new AtomicInteger(0);

    // Equilibrium monitoring: started vs. finished transactions.
    final AtomicInteger started = new AtomicInteger(0);
    final AtomicInteger finished = new AtomicInteger(0);

    // ---------------------------------------------------------------
    // Admission gate for WRITE transactions
    // ---------------------------------------------------------------

    /**
     * Normal WRITE attempts share the read lock.
     * A WRITE transaction that reaches the retry limit takes
     * the exclusive write lock and runs alone.
     *
     * Fair mode prevents new attempts from continually overtaking
     * a transaction waiting for exclusive access.
     */
    final ReentrantReadWriteLock gate =
            new ReentrantReadWriteLock(true);

    public NoLiveLocks(boolean persistent, EnumSet<Fix> fixes, boolean quiet, int seconds) {

        this.persistent = persistent;
        this.fixes = fixes;
        this.out = fixes.contains(Fix.BUFFERED_OUTPUT)
                ? new PrintStream(new BufferedOutputStream(
                new FileOutputStream(FileDescriptor.out), 1 << 16), false)
                : System.out;
        this.quiet = quiet;
        this.seconds = seconds;

        for (int i = 0; i < committed.length; i++) {
            committed[i] = new AtomicInteger(0);
        }
    }

    // ---------------------------------------------------------------
    // Database setup
    // ---------------------------------------------------------------

    void createDB() throws SQLException {

        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL);
             Statement st = con.createStatement()) {

            st.execute("DROP TABLE IF EXISTS blocking_data");

            st.execute(
                    "CREATE TABLE blocking_data (" +
                            "data_id integer primary key, " +
                            "data_value float, " +
                            "modified_by integer)" +
                            (fixes.contains(Fix.FILLFACTOR) ? " WITH (fillfactor = 70)" : "")
            );

            con.setAutoCommit(false);

            Random random = new Random(SEED);

            try (PreparedStatement ps =
                         con.prepareStatement(
                                 "INSERT INTO blocking_data VALUES (?, ?, ?)")) {

                for (int i = 0; i < num_tuple; i++) {

                    ps.setInt(1, i);
                    ps.setDouble(2, random.nextDouble() * max_value);
                    ps.setInt(3, 0);

                    ps.addBatch();
                }

                ps.executeBatch();
            }

            con.commit();
        }

        // VACUUM must run outside a transaction.
        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL);
             Statement st = con.createStatement()) {

            st.execute("VACUUM ANALYZE blocking_data");
        }
    }

    /**
     * Removes tuples inserted during the benchmark.
     */
    void cleanup() {

        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL);
             Statement st = con.createStatement()) {

            st.executeUpdate(
                    "DELETE FROM blocking_data WHERE data_id >= " + num_tuple
            );

        } catch (SQLException e) {

            System.err.println(
                    "Cleanup failed: " + e.getMessage()
            );
        }
    }

    // ---------------------------------------------------------------
    // Connections and prepared statements
    // ---------------------------------------------------------------

    Connection openConnection() throws SQLException {

        Connection con =
                ConnectionFactory.getDefaultParameterConnection(
                        ConnectionFactory.POSTGRESQL);

        try (Statement st = con.createStatement()) {

            if (fixes.contains(Fix.SYNC_COMMIT_OFF)) {
                st.execute("SET synchronous_commit = off");
            }
            if (fixes.contains(Fix.PARALLEL)) {
                st.execute("SET max_parallel_workers_per_gather = 4");
            }
            if (DEADLOCK_TIMEOUT_MS > 0) {
                // needs a superuser (the "postgres" user is one)
                st.execute("SET deadlock_timeout = '" + DEADLOCK_TIMEOUT_MS + "ms'");
            }
        }

        con.setAutoCommit(false);

        con.setTransactionIsolation(
                Connection.TRANSACTION_READ_COMMITTED
        );

        return con;
    }

    static class Stmts {

        final PreparedStatement point;
        final PreparedStatement insert;
        final PreparedStatement sum;
        final PreparedStatement update;

        Stmts(Connection con) throws SQLException {

            point = con.prepareStatement(
                    "SELECT data_value FROM blocking_data WHERE data_id = ?"
            );

            insert = con.prepareStatement(
                    "INSERT INTO blocking_data VALUES (?, ?, ?)"
            );

            sum = con.prepareStatement(
                    "SELECT sum(data_value) FROM blocking_data"
            );

            update = con.prepareStatement(
                    "UPDATE blocking_data " +
                            "SET data_value = ?, modified_by = ? " +
                            "WHERE data_id = ?"
            );
        }

        void close() {

            try {
                point.close();
                insert.close();
                sum.close();
                update.close();

            } catch (SQLException e) {

                System.err.println(
                        "Could not close prepared statements: " +
                                e.getMessage()
                );
            }
        }
    }

    // ---------------------------------------------------------------
    // Transaction
    // ---------------------------------------------------------------

    /**
     * One transaction.
     * Its parameters are generated once and remain the same during retries.
     */
    public class Blocker {

        final int task = txIds.incrementAndGet();

        final Type type;

        final int[] ids =
                new int[num_queries_per_task];

        final double[] newValues =
                new double[num_queries_per_task];

        int insertId;
        double insertValue;

        Blocker(Random rand) {

            double p = rand.nextDouble();

            // Randomly select the transaction type.
            if (p < 0.6) {

                type = Type.POINT_READ;

                // Generate three distinct tuple IDs.
                // Their order is intentionally not sorted.
                for (int i = 0; i < ids.length; i++) {

                    int id;
                    boolean duplicate;

                    do {

                        id = rand.nextInt(num_tuple);
                        duplicate = false;

                        for (int j = 0; j < i; j++) {

                            if (ids[j] == id) {
                                duplicate = true;
                                break;
                            }
                        }

                    } while (duplicate);

                    ids[i] = id;
                }

            } else if (p < 0.7) {

                type = Type.INSERT;

                insertId = nextInsertId.getAndIncrement();
                insertValue = rand.nextDouble() * max_value;

            } else if (p < 0.8) {

                type = Type.FULL_SCAN;

            } else {

                type = Type.WRITE;

                // Generate three distinct tuple IDs for updates.
                // Normally from all tuples; with -Dhot=N only from the first N.
                final int range = (HOT >= num_queries_per_task && HOT < num_tuple)
                        ? HOT : num_tuple;

                for (int i = 0; i < ids.length; i++) {

                    int id;
                    boolean duplicate;

                    do {

                        id = rand.nextInt(range);
                        duplicate = false;

                        for (int j = 0; j < i; j++) {

                            if (ids[j] == id) {
                                duplicate = true;
                                break;
                            }
                        }

                    } while (duplicate);

                    ids[i] = id;
                    newValues[i] = rand.nextDouble() * max_value;
                }
            }
        }

        /**
         * Executes the transaction statements.
         * Commit is handled separately by the worker.
         */
        void execute(Stmts s) throws SQLException {

            switch (type) {

                case POINT_READ: {

                    double sum = 0;

                    for (int id : ids) {

                        s.point.setInt(1, id);

                        try (ResultSet rs = s.point.executeQuery()) {

                            if (rs.next()) {
                                sum += rs.getDouble(1);
                            }
                        }
                    }

                    if (!quiet) {

                        out.println(
                                "Task " + task +
                                        " [point read] sum = " + sum
                        );
                    }

                    break;
                }

                case INSERT: {

                    s.insert.setInt(1, insertId);
                    s.insert.setDouble(2, insertValue);
                    s.insert.setInt(3, task);

                    s.insert.executeUpdate();

                    break;
                }

                case FULL_SCAN: {

                    try (ResultSet rs = s.sum.executeQuery()) {

                        if (rs.next() && !quiet) {

                            out.println(
                                    "Task " + task +
                                            " [full scan] sum = " +
                                            rs.getDouble(1)
                            );
                        }
                    }

                    break;
                }

                case WRITE: {

                    // Keep the generated order.
                    // Do not sort the IDs: this allows deadlocks to occur.

                    if (fixes.contains(Fix.BATCH_UPDATES)) {

                        // Same 3 updates in the same order, sent in one
                        // round trip instead of three.
                        s.update.clearBatch();

                        for (int i = 0; i < ids.length; i++) {

                            s.update.setDouble(1, newValues[i]);
                            s.update.setInt(2, task);
                            s.update.setInt(3, ids[i]);
                            s.update.addBatch();
                        }

                        s.update.executeBatch();

                    } else {

                        for (int i = 0; i < ids.length; i++) {

                            s.update.setDouble(1, newValues[i]);
                            s.update.setInt(2, task);
                            s.update.setInt(3, ids[i]);

                            s.update.executeUpdate();
                        }
                    }

                    break;
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Worker = Continuous Closed-Loop Stream
    // ---------------------------------------------------------------

    /**
     * Next transaction for a worker, or null when the stream is over.
     * Continuous stream: a fresh random transaction until the deadline.
     * Fixed stream: the next pre-generated transaction until the list is empty.
     */
    Blocker nextTransaction(Random rand) {

        if (fixedStream != null) {
            int i = fixedNext.getAndIncrement();
            return i < fixedStream.size() ? fixedStream.get(i) : null;
        }

        return System.nanoTime() < deadlineNanos ? new Blocker(rand) : null;
    }

    class Worker implements Runnable {

        final int id;
        final Random rand;

        Worker(int id) {
            this.id = id;
            this.rand = new Random(SEED + id);
        }

        @Override
        public void run() {

            Connection con = null;
            Stmts s = null;

            try {

                if (persistent) {
                    con = openConnection();
                    s = new Stmts(con);
                }

                Blocker t;
                while ((t = nextTransaction(rand)) != null) {

                    if (!persistent) {
                        con = openConnection();
                        s = new Stmts(con);
                    }

                    started.incrementAndGet();
                    try {
                        runUntilCommit(t, con, s);
                    } finally {
                        finished.incrementAndGet();
                    }

                    if (!persistent) {
                        s.close();
                        con.close();
                        s = null;
                        con = null;
                    }
                }

            } catch (Exception e) {

                // A dead worker silently lowers the thread count: make it visible.
                failed.incrementAndGet();
                System.err.println(
                        "Worker " + id + " stopped: " + e.getMessage()
                );

            } finally {

                if (s != null) {
                    s.close();
                }

                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException ignore) {
                    }
                }
            }
        }

        void runUntilCommit(Blocker t, Connection con, Stmts s) throws SQLException {

            int attempt = 0;

            while (true) {

                boolean exclusive =
                        (t.type == Type.WRITE && attempt >= max_shared_attempts);

                Lock l = null;

                if (t.type == Type.WRITE) {
                    l = exclusive ? gate.writeLock() : gate.readLock();
                    l.lock();
                }

                try {

                    t.execute(s);
                    con.commit();

                    committed[t.type.ordinal()].incrementAndGet();

                    if (exclusive) {
                        exclusiveRuns.incrementAndGet();
                    }

                    updateMax(maxRestarts, attempt);

                    return;

                } catch (SQLException e) {

                    try {
                        con.rollback();
                    } catch (SQLException ignore) {
                    }

                    if (!isRetryable(e)) {

                        failed.incrementAndGet();

                        System.err.println(
                                "Task " + t.task +
                                        " failed permanently: " + e.getMessage()
                        );

                        return;
                    }

                    attempt++;
                    totalRestarts.incrementAndGet();

                    if (!quiet) {

                        System.err.println(
                                "Task " + t.task +
                                        " aborted (" + e.getSQLState() +
                                        "), restart #" + attempt +
                                        (attempt >= max_shared_attempts
                                                ? " -> will run exclusively"
                                                : "")
                        );
                    }

                } finally {

                    if (l != null) {
                        l.unlock();
                    }
                }

                backoff(attempt);
            }
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    static boolean isRetryable(SQLException e) {

        for (SQLException c = e; c != null; c = c.getNextException()) {

            String state = c.getSQLState();

            if ("40P01".equals(state) || "40001".equals(state)) {
                return true;
            }
        }

        return false;
    }

    static void backoff(int attempt) {

        if (attempt <= 0) {
            return;
        }

        long max = Math.min(500L, 10L << Math.min(attempt, 5));

        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(max));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    static void updateMax(AtomicInteger max, int value) {

        int cur;

        while (value > (cur = max.get()) &&
                !max.compareAndSet(cur, value)) {
        }
    }

    // ---------------------------------------------------------------
    // Benchmark Runner & CLI
    // ---------------------------------------------------------------

    int totalCommitted() {
        int total = 0;
        for (AtomicInteger c : committed) {
            total += c.get();
        }
        return total;
    }

    /**
     * Prints one line per second: commits in the last second, started vs.
     * finished transactions and the number in flight. In a closed loop the
     * number in flight stays equal to the number of workers, and the
     * per-second rate levels off after a short warm-up. That steady state is
     * the equilibrium.
     */
    ScheduledExecutorService startMonitor(int threads, long start) {
        ScheduledExecutorService mon = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread th = new Thread(r, "monitor");
            th.setDaemon(true);
            return th;
        });
        final int[] last = {0};
        mon.scheduleAtFixedRate(() -> {
            int now = totalCommitted();
            int s = started.get(), f = finished.get();
            System.out.printf(
                    "[MONITOR t=%4.1fs] commits/s=%6d | started=%7d finished=%7d in-flight=%2d/%d | restarts=%d%n",
                    (System.nanoTime() - start) / 1e9, now - last[0], s, f, s - f, threads,
                    totalRestarts.get());
            last[0] = now;
        }, 1, 1, TimeUnit.SECONDS);
        return mon;
    }

    static double run(int threads, boolean optimized, boolean quiet, int seconds) throws Exception {
        return run(threads, optimized, quiet, seconds, true);
    }

    static double run(int threads, boolean optimized, boolean quiet, int seconds,
                      boolean monitor) throws Exception {
        return run(threads, optimized,
                optimized ? OPTIMIZED_FIXES : EnumSet.noneOf(Fix.class),
                quiet, seconds, monitor);
    }

    static double run(int threads, boolean persistent, EnumSet<Fix> fixes,
                      boolean quiet, int seconds, boolean monitor) throws Exception {
        return run(threads, persistent, fixes, quiet, seconds, monitor, 0)[0];
    }

    /**
     * Core runner. fixedCount == 0: continuous stream for 'seconds' seconds.
     * fixedCount  > 0: finite stream of exactly that many transactions
     * (generated from SEED, generation time is included in the measurement).
     *
     * @return {throughput in tx/s, elapsed seconds, committed, failed}
     */
    static double[] run(int threads, boolean persistent, EnumSet<Fix> fixes,
                        boolean quiet, int seconds, boolean monitor,
                        int fixedCount) throws Exception {

        NoLiveLocks b = new NoLiveLocks(persistent, fixes, quiet, seconds);
        b.createDB();

        // If the run is interrupted (Ctrl+C during the demo), still remove inserted tuples.
        Thread hook = new Thread(b::cleanup);
        Runtime.getRuntime().addShutdownHook(hook);

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        long start = System.nanoTime();
        if (fixedCount > 0) {
            // Same stream for every run: one Random, one seed.
            Random gen = new Random(SEED);
            b.fixedStream = new java.util.ArrayList<>(fixedCount);
            for (int i = 0; i < fixedCount; i++) {
                b.fixedStream.add(b.new Blocker(gen));
            }
            b.deadlineNanos = Long.MAX_VALUE;
        } else {
            b.deadlineNanos = start + seconds * 1_000_000_000L;
        }
        ScheduledExecutorService mon = monitor ? b.startMonitor(threads, start) : null;

        for (int i = 0; i < threads; i++) {
            executor.execute(b.new Worker(i));
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);

        double elapsed = (System.nanoTime() - start) / 1e9;
        b.out.flush();
        if (mon != null) {
            mon.shutdownNow();
        }
        b.cleanup();
        Runtime.getRuntime().removeShutdownHook(hook);

        int total = 0;
        for (AtomicInteger c : b.committed) {
            total += c.get();
        }

        double tps = total / elapsed;

        if (fixedCount > 0) {
            System.out.printf("[FIXED STREAM of %d transactions] ", fixedCount);
        }

        System.out.printf(
                "[DONE] threads=%d persistent=%b fixes=%s | %.1f s | committed=%d " +
                        "(read=%d insert=%d scan=%d write=%d) | throughput=%.1f tx/s | " +
                        "restarts=%d | maxRestarts=%d | exclusive=%d | failed=%d%n",
                threads, persistent, fixes, elapsed, total,
                b.committed[0].get(), b.committed[1].get(),
                b.committed[2].get(), b.committed[3].get(),
                tps, b.totalRestarts.get(), b.maxRestarts.get(),
                b.exclusiveRuns.get(), b.failed.get()
        );

        return new double[]{tps, elapsed, total, b.failed.get()};
    }

    public static void main(String[] args) throws Exception {

        String mode = args.length > 0 ? args[0] : "concurrent";
        int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 30;
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : NUM_THREADS;
        boolean optimized = args.length > 3 && Boolean.parseBoolean(args[3]);
        boolean quiet = args.length > 4 && Boolean.parseBoolean(args[4]);

        switch (mode) {

            case "serial":
                run(1, optimized, quiet, seconds);
                break;

            case "concurrent":
                run(threads, optimized, quiet, seconds);
                break;

            case "fixed": {
                // java NoLiveLocks fixed [count] [threads] [optimized] [quiet]
                // Serial vs concurrent execution of the SAME finite stream.
                // (the 2nd CLI argument is the number of transactions here)
                int count = args.length > 1 ? Integer.parseInt(args[1]) : 20000;
                EnumSet<Fix> f = optimized ? OPTIMIZED_FIXES : EnumSet.noneOf(Fix.class);

                // Both runs use persistent connections: connection creation is
                // excluded from Part 2.
                double[] serial = run(1, true, f, quiet, 0, false, count);
                double[] conc = run(threads, true, f, quiet, 0, false, count);

                System.out.printf(
                        "%n===== SERIAL vs CONCURRENT, same %d transactions =====%n" +
                                "serial     (1 thread) : %8.2f s  %10.1f tx/s%n" +
                                "concurrent (%d threads): %8.2f s  %10.1f tx/s%n" +
                                "speed-up               : %.2fx%n",
                        count, serial[1], serial[0], threads, conc[1], conc[0],
                        serial[1] / conc[1]);
                break;
            }

            case "scale": {
                System.out.println("Hardware threads (logical processors): "
                        + Runtime.getRuntime().availableProcessors());
                int[] counts = {1, 2, 4, 8, 16};
                double baseSerial = 0;

                StringBuilder table = new StringBuilder();
                table.append(String.format(
                        "%-10s %-8s %-12s %-18s %-22s%n",
                        "optimized", "threads", "tx/s",
                        "speedup vs 1 thr.", "speedup vs serial base"
                ));

                for (boolean opt : new boolean[]{false, true}) {
                    double one = 0;

                    for (int t : counts) {
                        double tps = run(t, opt, true, seconds, false);
                        if (t == 1) one = tps;
                        if (!opt && t == 1) baseSerial = tps;

                        table.append(String.format(
                                "%-10b %-8d %-12.1f %-18.2f %-22.2f%n",
                                opt, t, tps, tps / one, tps / baseSerial
                        ));
                    }
                }

                System.out.println("\n===== SCALING RESULTS (" + seconds + " s per run) =====\n" + table);
                break;
            }

            case "ablation": {
                // java NoLiveLocks ablation [seconds] [threads] [runs]
                // Adds one fix at a time. All rows use persistent connections,
                // because connection creation is excluded from Part 2.
                int runs = args.length > 3 ? Integer.parseInt(args[3]) : 2;

                EnumSet<Fix> none = EnumSet.noneOf(Fix.class);
                EnumSet<Fix> all = EnumSet.allOf(Fix.class);
                Object[][] configs = {
                        {"serial (1 thread), no fixes", 1, none},
                        {"serial (1 thread), all fixes", 1, all},
                        {threads + " threads, no fixes", threads, none},
                        {"  + synchronous_commit off", threads,
                                EnumSet.of(Fix.SYNC_COMMIT_OFF)},
                        {"  + batched updates", threads,
                                EnumSet.of(Fix.SYNC_COMMIT_OFF, Fix.BATCH_UPDATES)},
                        {"  + buffered output", threads,
                                EnumSet.of(Fix.SYNC_COMMIT_OFF, Fix.BATCH_UPDATES,
                                        Fix.BUFFERED_OUTPUT)},
                        {"  + fillfactor 70", threads,
                                EnumSet.of(Fix.SYNC_COMMIT_OFF, Fix.BATCH_UPDATES,
                                        Fix.BUFFERED_OUTPUT, Fix.FILLFACTOR)},
                        {"  + parallel workers (= all fixes)", threads, all},
                };

                StringBuilder table = new StringBuilder();
                table.append(String.format("%-36s %12s %12s %12s%n",
                        "configuration", "tx/s (avg)", "vs serial", "vs prev row"));
                double serial = 0, prev = 0;

                for (Object[] c : configs) {
                    @SuppressWarnings("unchecked")
                    EnumSet<Fix> f = (EnumSet<Fix>) c[2];
                    double sum = 0;
                    for (int r = 0; r < runs; r++) {
                        sum += run((Integer) c[1], true, f, quiet, seconds, false);
                    }
                    double avg = sum / runs;
                    if (serial == 0) serial = avg;
                    table.append(String.format("%-36s %12.1f %11.2fx %12s%n",
                            c[0], avg, avg / serial,
                            prev == 0 ? "" : String.format("%+.1f%%", (avg / prev - 1) * 100)));
                    // The second row (serial, all fixes) is not part of the chain.
                    prev = (c[0].toString().startsWith("serial (1 thread), all")) ? 0 : avg;
                }

                System.out.println("\n===== ABLATION (" + seconds + " s x " + runs
                        + " runs per row, persistent connections) =====\n" + table);
                break;
            }

            default:
                System.err.println(
                        "Usage: java [-Dhot=N] [-DdeadlockTimeoutMs=N] NoLiveLocks [serial|concurrent|scale|ablation|fixed] [seconds|count] [threads] [optimized|runs] [quiet]"
                );
        }
    }
}