
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RestartBlocking {

    // Experiment settings
    private static final int NUM_TASKS = 30;
    private static final int NUM_QUERIES_PER_TASK = 3;
    private static final int NUM_THREADS = 4;

    private static final int NUM_TUPLES = 50000;
    private static final double MAX_VALUE = 50.0;

    private final AtomicInteger tasksFinished = new AtomicInteger(0);
    private final AtomicInteger totalRetries = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {

        RestartBlocking program = new RestartBlocking();

        // Recreate and populate the database table
        try {
            program.createDB();
        } catch (SQLException e) {
            System.err.println("Could not create the database table.");
            e.printStackTrace();
            return;
        }

        // Create the thread pool
        ExecutorService executor =
                Executors.newFixedThreadPool(NUM_THREADS);

        long start = System.currentTimeMillis();

        // Submit all tasks
        for (int task = 0; task < NUM_TASKS; task++) {
            executor.execute(program.new Blocker(task));
        }

        executor.shutdown();

        // Wait until all tasks have finished
        try {
            if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
                System.err.println("Tasks did not finish within the time limit.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw e;
        }

        long stop = System.currentTimeMillis();

        System.out.println("\n========== RESULTS ==========");
        System.out.println("Execution time: " + (stop - start) + " ms");
        System.out.println("Successfully finished tasks: "
                + program.tasksFinished.get() + " of " + NUM_TASKS);
        System.out.println("Total deadlock retries: "
                + program.totalRetries.get());
        System.out.println("=============================");
    }

    // Create and populate the database table
    private void createDB() throws SQLException {

        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL);
             Statement stmt = con.createStatement()) {

            stmt.execute("DROP TABLE IF EXISTS blocking_data");

            stmt.execute(
                    "CREATE TABLE blocking_data (" +
                            "data_id integer PRIMARY KEY, " +
                            "data_value float, " +
                            "modified_by integer)"
            );

            con.setAutoCommit(false);

            String sql =
                    "INSERT INTO blocking_data VALUES (?, ?, ?)";

            try (PreparedStatement ps = con.prepareStatement(sql)) {

                for (int i = 0; i < NUM_TUPLES; i++) {

                    ps.setInt(1, i);

                    ps.setDouble(
                            2,
                            ThreadLocalRandom.current().nextDouble()
                                    * MAX_VALUE
                    );

                    ps.setInt(3, 0);

                    ps.addBatch();
                }

                ps.executeBatch();
                con.commit();
            }
        }

        System.out.println("Database table created with "
                + NUM_TUPLES + " rows.");
    }

    // Check whether the SQL exception is a PostgreSQL deadlock
    private boolean isDeadlock(SQLException exception) {

        SQLException current = exception;

        while (current != null) {

            if ("40P01".equals(current.getSQLState())) {
                return true;
            }

            current = current.getNextException();
        }

        return false;
    }

    // Each task executes several UPDATE queries in one transaction
    public class Blocker implements Runnable {

        private final int task;
        private final double[] startRange;
        private final double[] stopRange;

        public Blocker(int task) {

            this.task = task;

            this.startRange =
                    new double[NUM_QUERIES_PER_TASK];

            this.stopRange =
                    new double[NUM_QUERIES_PER_TASK];

            for (int q = 0; q < NUM_QUERIES_PER_TASK; q++) {

                stopRange[q] =
                        ThreadLocalRandom.current().nextDouble()
                                * MAX_VALUE;

                startRange[q] =
                        ThreadLocalRandom.current().nextDouble()
                                * stopRange[q];
            }
        }

        @Override
        public void run() {

            int retries = 0;

            try (Connection con =
                         ConnectionFactory.getDefaultParameterConnection(
                                 ConnectionFactory.POSTGRESQL)) {

                con.setAutoCommit(false);

                // Retry the complete transaction after a deadlock
                while (!Thread.currentThread().isInterrupted()) {

                    try {

                        String sql =
                                "UPDATE blocking_data " +
                                        "SET data_value = ?, modified_by = ? " +
                                        "WHERE data_value >= ? " +
                                        "AND data_value <= ?";

                        try (PreparedStatement ps =
                                     con.prepareStatement(
                                             sql,
                                             ResultSet.TYPE_SCROLL_SENSITIVE,
                                             ResultSet.CONCUR_UPDATABLE)) {

                            for (int q = 0;
                                 q < NUM_QUERIES_PER_TASK;
                                 q++) {

                                ps.setDouble(
                                        1,
                                        ThreadLocalRandom.current()
                                                .nextDouble() * MAX_VALUE
                                );

                                ps.setInt(2, task);
                                ps.setDouble(3, startRange[q]);
                                ps.setDouble(4, stopRange[q]);

                                ps.executeUpdate();
                            }
                        }

                        // All queries succeeded: commit the transaction
                        con.commit();

                        int finished =
                                tasksFinished.incrementAndGet();

                        totalRetries.addAndGet(retries);

                        System.out.println(
                                "Task " + task +
                                        " committed successfully. " +
                                        "Retries: " + retries +
                                        ". Finished tasks: " + finished
                        );

                        return;

                    } catch (SQLException e) {

                        // A failed PostgreSQL transaction must be rolled back
                        try {
                            con.rollback();
                        } catch (SQLException rollbackError) {
                            e.addSuppressed(rollbackError);
                            throw e;
                        }

                        if (isDeadlock(e)) {

                            retries++;

                            System.out.println(
                                    "Deadlock detected in task " + task +
                                            ". Retrying transaction. Attempt: "
                                            + retries
                            );

                            // Short increasing delay with random variation
                            long delay = Math.min(
                                    100L,
                                    5L * (1L << Math.min(retries - 1, 4))
                            );

                            delay += ThreadLocalRandom.current()
                                    .nextLong(21);

                            try {
                                Thread.sleep(delay);
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                return;
                            }

                        } else {

                            // Do not retry errors that are not deadlocks
                            throw e;
                        }
                    }
                }

            } catch (SQLException e) {

                System.err.println(
                        "Task " + task +
                                " failed: " + e.getMessage()
                );
            }
        }
    }
}
