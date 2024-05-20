package ru.otus.java.basic.tegneryadnov.coursework;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Утилита многопотчного запуска распараллеленного SQL запроса, с записью результатов в файл отчета
 */
public class MainApp {
    static {
        AppSettings.init(SettingsType.XML_FILE, "db-upploader.properties.xml");
    }
    public static final long timeStart = System.currentTimeMillis() / 1000;
    public static BlockingQueue<String> rowsQueue = new LinkedBlockingQueue<>(AppSettings.getInt("QUEUE_CAPACITY"));

    public static void main(String[] args) {
        new Thread(new Consumer()).start();
        (new DataProducer()).execute();
    }
}
