package ru.otus.java.basic.tegneryadnov.coursework;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;

import static ru.otus.java.basic.tegneryadnov.coursework.MainApp.rowsQueue;
import static ru.otus.java.basic.tegneryadnov.coursework.MainApp.timeStart;

/**
 * Класс читателя из очереди сообщений
 * Пока в очереди не появится сообщение POISON_PILL, в потоке опрашивает очередь на предмет новых записей.
 * Открывает файл отчета на запись и построчно заполняет сообщениями из очереди
 */
public class Consumer implements Runnable {
    private final BufferedWriter bufferedWriter;
    private final static String POISON_PILL = "POISON_PILL";
    private static final Logger logger = LogManager.getLogger(Consumer.class.getName());

    public Consumer() {
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(AppSettings.getString("REPORT_FILE_NAME"), Charset.forName(AppSettings.getString("REPORTER_CHARSET_NAME"))));
            logger.info(String.format("Старт выгрузки в файл отчета: %s", AppSettings.getString("REPORT_FILE_NAME")));
        } catch (IOException e) {
            logger.error(String.format("Ошибка отрытия файла отчета: %s %s", AppSettings.getString("REPORT_FILE_NAME"), e));
            throw new RuntimeException(e);
        }
    }

    public void run() {
        try {
            int lineCounter = 0;
            while (true) {
                String row = rowsQueue.take();
                if (row.equals(POISON_PILL)) {
                    logger.debug("Поступила команда: закончить чтение из очереди");
                    bufferedWriter.flush();
                    bufferedWriter.close();
                    logger.info(String.format("Выгрузка в файл %s завершена.", AppSettings.getString("REPORT_FILE_NAME")));
                    logger.info(String.format("Выгружено записей: %d", lineCounter));
                    logger.debug(String.format("Время работы программы: %d", System.currentTimeMillis() / 1000 - timeStart));
                    return;
                }
                try {
                    bufferedWriter.write(row + "\r\n");
                    bufferedWriter.flush();
                    lineCounter++;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (InterruptedException | IOException e) {
            Thread.currentThread().interrupt();
        }
    }
}