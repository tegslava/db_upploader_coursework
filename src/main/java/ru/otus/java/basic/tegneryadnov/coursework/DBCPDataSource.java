package ru.otus.java.basic.tegneryadnov.coursework;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Класс-пул соединений
 */
public class DBCPDataSource {
    private static final BasicDataSource ds = new BasicDataSource();

    static {
        ds.setUrl(AppSettings.getString("URL"));
        ds.setUsername(AppSettings.getString("LOGIN"));
        ds.setPassword(AppSettings.getString("PASSWORD"));
        ds.setMinIdle(5);
        ds.setMaxIdle(10);
        ds.setMaxOpenPreparedStatements(AppSettings.getInt("THREADS_COUNT"));
    }

    /**
     * Метод получения нового соединения к БД
     *
     * @return возвращает соединение типа java.sql.Connection
     * @throws SQLException проблемы с открытием соединения
     */
    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}