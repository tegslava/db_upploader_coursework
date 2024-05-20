package ru.otus.java.basic.tegneryadnov.coursework;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Класс чтения и хранения настроек программы.
 * Источник загрузки настроек определяется типом SettingsType
 * Реализована загрузка параметров из XML файла
 */
public class AppSettings implements ReLoadable {

    private static final Map<String, String> hashMap = new HashMap<>();
    private static AppSettings appSettings;
    private final String fileName;
    private final SettingsType settingsSourceType;
    private static final Logger logger = LogManager.getLogger(AppSettings.class.getName());

    private AppSettings(SettingsType settingsSourceType, String fileName) {
        this.fileName = fileName;
        this.settingsSourceType = settingsSourceType;
        load();
    }

    public static void init(SettingsType settingsSourceType, String fileName) {
        if (appSettings == null) {
            appSettings = new AppSettings(settingsSourceType, fileName);
        }
    }

    /**
     * Метод чтения текстового значения параметра key
     *
     * @param key   имя параметра
     * @param deflt значение по умолчанию
     * @return значение параметра
     */
    public static String getString(String key, String deflt) {
        String value = hashMap.get(key);
        if (value == null) {
            return deflt;
        } else {
            return value;
        }
    }

    /**
     * Метод чтения текстового значения параметра key
     *
     * @param key имя параметра
     * @return значение параметра
     */
    public static String getString(String key) {
        return getString(key, "");
    }

    /**
     * Метод чтения целочисленного значения параметра key
     *
     * @param key имя параметра
     * @return значение параметра
     */
    public static int getInt(String key) {
        try {
            return Integer.parseInt(hashMap.get(key));
        } catch (NumberFormatException e) {
            throw new RuntimeException(String.format("Ошибочный параметр для \"%s\"", key));
        }
    }

    /**
     * Запись в хранилище текстовой пары ключ-значение параметра
     *
     * @param key  имя параметра
     * @param data значение параметра
     */
    public static void putString(String key, String data) {
        if (data == null) {
            throw new IllegalArgumentException();
        } else {
            hashMap.put(key, data);
        }
    }

    /**
     * Загрузка параметров в хранилище из XML файла
     *
     * @param fileName имя файла
     * @throws ParserConfigurationException проблемы с парсингом XML
     * @throws IOException                  проблемы с открытием файла XML
     * @throws SAXException                 проблемы с парсингом XML
     */
    public static void loadFromXML(String fileName) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new File(fileName));
        Node root = doc.getDocumentElement();
        NodeList nodeList = root.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (nodeList.item(i).getNodeName().equals("properties")) {
                NodeList propertyList = nodeList.item(i).getChildNodes();
                for (int j = 0; j < propertyList.getLength(); j++) {
                    NamedNodeMap attributes = propertyList.item(j).getAttributes();
                    if (attributes != null) {
                        Node n = attributes.getNamedItem("key");
                        NodeList childs = propertyList.item(j).getChildNodes();
                        for (int k = 0; k < childs.getLength(); k++) {
                            if (childs.item(k).getNodeType() == Node.TEXT_NODE) {
                                putString(n.getNodeValue(), childs.item(k).getNodeValue());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Загрузка параметров программы из источника в хранилище. Тип источника определяется settingsSourceType.
     * Дополнительно, для обеспечения безопасности, из строки запуска забираем логин и пароль подключения к БД.
     */
    @Override
    public void load() {
        switch (settingsSourceType) {
            case PROPERTIES: {
            }
            break;
            default:
                try {
                    loadFromXML(fileName);
                } catch (ParserConfigurationException e) {
                    logger.error(String.format("Ошибка загрузки настроек программы %s", e));
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    logger.error(String.format("Ошибка чтения файла загрузки настроек программы: %s %s", fileName, e));
                    throw new RuntimeException(e);
                } catch (SAXException e) {
                    logger.error(e);
                    throw new RuntimeException(e);
                }
        }
        putString("LOGIN", (String) System.getProperties().getOrDefault("login", "defautlLogin"));
        putString("PASSWORD", (String) System.getProperties().getOrDefault("password", "defautlPassword"));
        logger.info(String.format("Настройки загружены из файла: %s", fileName));
    }
}