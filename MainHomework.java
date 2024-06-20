import java.sql.Connection;
import java.sql.*;
import java.sql.DriverManager;

public class MainHomework {

    public static void main(String[] args) {

        Homework homework = new Homework();

        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
            homework.createPersonTable(connection);
            homework.insertPersonData(connection);
            homework.createDepartmentTable(connection);
            homework.insertDepartmentData(connection);
            homework.selectDepartmentData(connection);
            homework.alterPersonColumn(connection);
            homework.selectPersonData(connection);
            System.out.println(homework.getPersonDepartmentName(connection, 4));
            System.out.println(homework.getPersonDepartments(connection));
            System.out.println(homework.getDepartmentPersons(connection));

        } catch (SQLException e) {
            System.err.println("Во время подключения произошла ошибка: " + e.getMessage());
        }
    }
}