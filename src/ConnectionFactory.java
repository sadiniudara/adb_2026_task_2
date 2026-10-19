
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class ConnectionFactory {

    public static final int POSTGRESQL = 1;

    private static final String URL =
            "jdbc:postgresql://localhost:5432/blockingdb";

    private static final String USER = "postgres";

    private static final String PASSWORD = "Rituriya@03";

    private ConnectionFactory() {
    }

    public static Connection getDefaultParameterConnection(int database) {

        if (database != POSTGRESQL) {
            throw new IllegalArgumentException("Unsupported database");
        }

        try {
            return DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "PostgreSQL connection failed: " + e.getMessage(), e
            );
        }
    }
}

