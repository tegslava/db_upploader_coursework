package ru.otus.java.basic.tegneryadnov.coursework;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Класс для запуска потоков с параллельными запросами к БД.
 * Количество запущенных запросов определяется параметром из настроек threadsCount
 * Результаты работы каждого запроса в потоке построчно выгружается в очередь сообщений rowsQueue
 * После окончания работы всех запросов в очередь посылается сообщение "отравленная пилюля" poisonPill -
 * сигнал читателю очереди окончить работу
 */
public class DataProducer {
    private final AppSettings appSettings;
    private final BlockingQueue<String> rowsQueue;
    private final ExecutorService services;
    private boolean headerChecked = false;
    private final String POISON_PILL;
    private final String COLUMN_SEPARATOR;
    private final String SQL;
    private final int THREADS_COUNT;
    private final String WITH_HEADER;
    private static final Logger logger = LogManager.getLogger(DataProducer.class.getName());

    public DataProducer(BlockingQueue<String> rowsQueue, AppSettings appSettings) {
        this.rowsQueue = rowsQueue;
        this.appSettings = appSettings;
        POISON_PILL = appSettings.getString("poisonPill", "unknownPoisonPill");
        COLUMN_SEPARATOR = appSettings.getString("reportColumnSeparator", ";");
        SQL = appSettings.getString("sql", "");
        THREADS_COUNT = appSettings.getInt("threadsCount");
        WITH_HEADER = appSettings.getString("reportWithHeader", "Y");
        services = Executors.newFixedThreadPool(THREADS_COUNT);
    }

    private void sendCommandStopConsumer() {
        try {
            rowsQueue.put(POISON_PILL);
        } catch (InterruptedException e) {
            logger.error(String.format("Ошибка отправки POISON_PILL %s", e));
            throw new RuntimeException(e);
        }
    }

    /**
     * Сформировать шапку csv файла
     *
     * @param rsmd ResultSetMetaData записи
     * @return возвращает строку из колонок с разделителем
     * @throws SQLException проблемы с разбором струткуры ResultSetMetaData
     */
    private String getHeader(ResultSetMetaData rsmd) throws SQLException {
        if (rsmd == null) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            stringBuilder.append(rsmd.getColumnName(i))
                    .append(i < rsmd.getColumnCount() ? COLUMN_SEPARATOR : "");
        }
        return stringBuilder.toString();
    }

    /**
     * Метод запускает потоки с параллельными запросами к БД.
     * Количество запущенных запросов определяется параметром из настроек THREADS_COUNT
     * Результаты работы запроса построчно выгружается в очередь сообщений rowsQueue
     */
    public void execute() {
        try {
            for (int i = 1; i <= THREADS_COUNT; i++) {
                int threadNumber = i;
                services.execute(() -> uploadDataIntoQueue(threadNumber));
            }
        } finally {
            if (services != null) {
                services.shutdown();
                try {
                    services.awaitTermination(30, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    logger.error(String.format("Ошибка закрытия пула потоков вычислений: %s", e));
                    //throw new RuntimeException(e);
                } finally {
                    sendCommandStopConsumer();
                }
                logger.info("Потоки запросов завершены");
            }
        }
    }

    /**
     * Запускает параллельный запрос, параметром которого является номер потока threadNumber - 1
     * Выполняет построчную обработку результата запроса, с заливкой в очередь сообщений
     * При необходимости формирует шапку отчета
     *
     * @param threadNumber номер потока
     */
    private void uploadDataIntoQueue(int threadNumber) {
        try (Connection connection = DBCPDataSource.getConnection(appSettings);
             PreparedStatement ps = connection.prepareStatement(SQL)) {
            int recordCounter = 0;
            ps.setInt(1, THREADS_COUNT);
            ps.setInt(2, threadNumber - 1);
            try (ResultSet resultSet = ps.executeQuery()) {
                StringBuilder stringBuilder = new StringBuilder();
                ResultSetMetaData resultSetMetaData = null;
                while (resultSet.next()) {
                    stringBuilder.setLength(0);
                    if (resultSetMetaData == null) {
                        resultSetMetaData = resultSet.getMetaData();
                    }
                    if (!headerChecked) {
                        processHeader(resultSetMetaData);
                    }
                    processRow(resultSetMetaData, stringBuilder, resultSet);
                    recordCounter++;
                }
                logger.info(String.format("Потоком %d залито записей: %d", threadNumber, recordCounter));
            }
        } catch (SQLException e) {
            logger.error(String.format("Ошибка SQL запроса в uploadDataIntoQueue(%d): %s", threadNumber, e));
            throw new RuntimeException(e);
        }
    }

    /**
     * Получает запись результата запроса, формирует строку с разделителем COLUMN_SEPARATOR.
     * Выгружает строку в очередь сообщений
     *
     * @param rsmd ResultSetMetaData записи
     * @param sb   StringBuilder
     * @param rs   ResultSet
     * @throws SQLException проблемы с разбором струткуры ResultSetMetaData
     */
    private void processRow(ResultSetMetaData rsmd, StringBuilder sb, ResultSet rs) throws SQLException {
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            sb.append(rs.getString(i))
                    .append(i < rsmd.getColumnCount() ? COLUMN_SEPARATOR : "");
        }
        try {
            rowsQueue.put(sb.toString());
        } catch (InterruptedException e) {
            logger.error(String.format("Ошибка отправки строки в очередь: %s", e));
            throw new RuntimeException(e);
        }
    }

    /**
     * Получает ResultSetMetaData записи результата запроса, выставляет флаг проверки обработки шапки отчета headerChecked
     * Если проверки не было и требуется шапка отчета WITH_HEADER == true, получает шапку, заливает в очередь
     *
     * @param rsmd ResultSetMetaData
     * @throws SQLException проблемы с разбором струткуры ResultSetMetaData
     */
    private synchronized void processHeader(ResultSetMetaData rsmd) throws SQLException {
        if (headerChecked) {
            return;
        }
        if (WITH_HEADER.equals("Y")) {
            String header = getHeader(rsmd);
            try {
                rowsQueue.put(header);
            } catch (InterruptedException e) {
                logger.error(String.format("Ошибка отправки заголовка в очередь: %s", e));
                throw new RuntimeException(e);
            }
        }
        headerChecked = true;
    }
}
