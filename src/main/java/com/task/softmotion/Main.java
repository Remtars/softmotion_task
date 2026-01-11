package com.task.softmotion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        // Параметры подключения к postgresql
        String url = "jdbc:postgresql://localhost:5432/softmotion_xml";
        String user = "postgres";
        String password = "123456";

        try {
            // Подключаемся к БД
            Connection connection = DriverManager.getConnection(url, user, password);
            XmlProcessor processor = new XmlProcessor(connection);

            // Пример 1: Получение списка таблиц
            System.out.println("[1] Available tables: " + processor.getTableNames());

            // Пример 2: Получение DDL для создания таблиц
            for (String tableName : processor.getTableNames()) {
                System.out.println("[2] DDL for table " + tableName + ":");
                System.out.println(processor.getTableDDL(tableName));
            }

            // Пример 3: Обновление всех таблиц
            System.out.println("[3] Data updating...");
            processor.update();
            System.out.println("[3] Data updated successfully");

            // Пример 4: Получение имен колонок
            String tableName = "offers";
            System.out.println("[4] Table " + tableName + " columns: " +
                    processor.getColumnNames(tableName));

            // Пример 5: Проверка уникальности колонки
            String columnName = "vendor_code";
            boolean isUnique = processor.isColumnId("offers", columnName);
            System.out.println("[5] Column " + columnName + " is unique: " + isUnique);

            // Пример 6: Получение DDL для изменения таблицы, добавление столбца
            System.out.println("[6] DDLChange for table " + tableName + ":");
            System.out.println(processor.getDDLChange(tableName));

            processor.close();

        } catch (SQLException e) {
            System.err.println("Error connecting to DB: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}