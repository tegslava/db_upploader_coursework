package ru.otus.java.basic.tegneryadnov.coursework;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Утилита многопотчного запуска распараллеленного SQL запроса, с записью результатов в файл отчета
 */
public class MainApp {
    private static final long timeStart = System.currentTimeMillis() / 1000;
    public static void main(String[] args) {
        AppSettings settings = AppSettings.getInstance(SettingsType.XML_FILE, "db-upploader.properties.xml");
        BlockingQueue<String> rowsQueue = new LinkedBlockingQueue<>(settings.getInt("queueCapacity"));
        new Thread(new Consumer(rowsQueue, settings, timeStart)).start();
        (new DataProducer(rowsQueue, settings)).execute();
    }
}
