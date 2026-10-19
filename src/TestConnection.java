
import java.sql.Connection;
import java.sql.SQLException;

public class TestConnection {

    public static void main(String[] args) {

        try (Connection connection =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL)) {

            System.out.println("Successfully connected to PostgreSQL!");
            System.out.println("Database: " +
                    connection.getCatalog());

        } catch (SQLException e) {
            System.err.println("Connection test failed:");
            e.printStackTrace();
        }
    }
}
