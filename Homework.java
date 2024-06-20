import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Homework {

    /**
     * С помощью JDBC, выполнить следующие пункты:
     * 1. Создать таблицу Person (скопировать код с семниара)
     * 2. Создать таблицу Department (id bigint primary key, name varchar(128) not null)
     * 3. Добавить в таблицу Person поле department_id типа bigint (внешний ключ)
     * 4. Написать метод, который загружает Имя department по Идентификатору person
     * 5. * Написать метод, который загружает Map<String, String>, в которой маппинг person.name -> department.name
     *   Пример: [{"person #1", "department #1"}, {"person #2", "department #3}]
     * 6. ** Написать метод, который загружает Map<String, List<String>>, в которой маппинг department.name -> <person.name>
     *   Пример:
     *   [
     *     {"department #1", ["person #1", "person #2"]},
     *     {"department #2", ["person #3", "person #4"]}
     *   ]
     *
     *  7. *** Создать классы-обертки над таблицами, и в пунктах 4, 5, 6 возвращать объекты.
     */

    /**
     * Пункт 1
     */
    public void createPersonTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table person (
                      id bigint primary key,
                      name varchar(256),
                      age integer,
                      active boolean
                    )
                    """);
        }
    }

    public void insertPersonData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into person(id, name, age, active) values\n");
            for (int i = 1; i <= 10; i++) {
                int age = ThreadLocalRandom.current().nextInt(20, 60);
                boolean active = ThreadLocalRandom.current().nextBoolean();
                insertQuery.append(String.format("(%s, '%s', %s, %s)", i, "Person #" + i, age, active));

                if (i != 10) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк: " + insertCount);
        }
    }

    public void selectPersonData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select id, name, age, active, department_id
                    from person""");

            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                boolean active = resultSet.getBoolean("active");
                long dep_id = resultSet.getLong("department_id");

                System.out.println("[id = " + id + ", name = " + name + ", age = " + age + ", active = " + active + ", depatment_id = " + dep_id + "]");
            }
        }
    }

    /**
     * Пункт 2
     */
    public void createDepartmentTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table department (
                      id bigint primary key,
                      name varchar(128) not null
                    )
                    """);
        }
    }

    public void insertDepartmentData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into department(id, name) values\n");
            for (int i = 1; i <= 3; i++) {
                insertQuery.append(String.format("(%s, '%s')", i, "Department #" + i));

                if (i != 3) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк: " + insertCount);
        }
    }

    public void selectDepartmentData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select id, name from department");
            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                System.out.println("[id = " + id + ", name = " + name + ", age = " + "]");
            }
        }
    }

    /**
     * Пункт 3
     */
    public int getRowsCount(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select count (1) from " + tableName);
            rs.next();
            return rs.getInt(1);
        }
    }


    public void alterPersonColumn(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("alter table person add department_id bigint");

            int countRows = getRowsCount(connection, "person");
            for (int i = 1; i <= countRows; i++) {
                int idDepartment = ThreadLocalRandom.current().nextInt(1, 4);
                statement.executeUpdate("update person set department_id = " + idDepartment + " where id = " + i);

            }
            System.out.println("В таблицу person добавлен столбец department_id и заполнен случайными значениями");
        }
    }


    /**
     * Пункт 4
     */
    public String getPersonDepartmentName(Connection connection, long personId) throws SQLException {
        String nameDep = "";
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(
                    "select name from department where id = select department_id from person where person.id = "
                            + personId);

            while (resultSet.next()) {
                nameDep = resultSet.getString("name");
                System.out.print("Сотрудник с ID = " + personId + " состоит в департаменте ");
                // throw new UnsupportedOperationException();
            }

        }

        return nameDep;
    }

    /**
     * Пункт 5
     */
    public Map<String, String> getPersonDepartments(Connection connection) throws SQLException {
        Map<String, String> personDepMap = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select person.name name, dep.name dep from 
                    (select id,  name, department_id from person) as person 
                    left join 
                    (select id, name from department) as dep  on  person.department_id = dep.id""");
            while (resultSet.next()) {
                String namePerson = resultSet.getString("name");
                String nameDep = resultSet.getString("dep");

                if (!personDepMap.containsKey(namePerson)) {
                    personDepMap.put(namePerson, nameDep);
                }
                //personDepMap.computeIfAbsent(namePerson, data -> nameDep);
            }
        }
        return personDepMap;
    }

    /**
     * Пункт 6
     */
    public Map<String, List<String>> getDepartmentPersons(Connection connection) throws SQLException {
        Map<String, List<String>> departmentPersMap = new HashMap<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select person.name name, dep.name dep from 
                    (select id,  name, department_id from person) as person 
                    left join 
                    (select id, name from department) as dep  on  person.department_id = dep.id""");
            while (resultSet.next()) {
                String namePers = resultSet.getString("name");
                String nameDep = resultSet.getString("dep");

                if (!departmentPersMap.containsKey(nameDep)) {
                    departmentPersMap.put(nameDep, new ArrayList<>());
                    departmentPersMap.get(nameDep).add(namePers);
                } else {
                    departmentPersMap.get(nameDep).add(namePers);
                }
            }

        }
        return departmentPersMap;
    }
}