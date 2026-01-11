package com.task.softmotion;

import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import groovy.xml.slurpersupport.NodeChild;
import org.xml.sax.InputSource;

import java.math.BigDecimal;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class XmlProcessor {
    private String xmlUrl = "https://expro.ru/bitrix/catalog_export/export_Sai.xml";
    private Connection connection;
    private GPathResult xmlData;

    public XmlProcessor(Connection connection) {
        this.connection = connection;
    }

    /**
     * Загружает XML с удаленного URL
     */
    public void loadXml() {
        try {
            URL url = new URL(xmlUrl);
            URLConnection connection = url.openConnection();
            XmlSlurper slurper = createSecureXmlSlurper();
            xmlData = slurper.parse(connection.getInputStream());
        } catch (Exception e) {
            throw new RuntimeException("Ошибка загрузки XML: " + e.getMessage(), e);
        }
    }

    private XmlSlurper createSecureXmlSlurper() {
        try {
            // Создаем XmlSlurper с безопасными настройками
            XmlSlurper slurper = new XmlSlurper();

            // Получаем доступ к SAXParserFactory для настройки
            // В Groovy 3.x XmlSlurper использует SAX парсер
            // Устанавливаем безопасные свойства
            slurper.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
            slurper.setFeature("http://xml.org/sax/features/external-general-entities", false);
            slurper.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            slurper.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

            return slurper;
        } catch (Exception e) {
            throw new RuntimeException("Ошибка при создании XMLSlurper: " + e.getMessage(), e);
        }
    }

    /**
     * Возвращает названия таблиц из XML (currency, categories, offers)
     * @return ArrayList
     */
    public ArrayList<String> getTableNames() {
        if (xmlData == null) {
            loadXml();
        }

        ArrayList<String> tables = new ArrayList<>();
        tables.add("currency");
        tables.add("categories");
        tables.add("offers");

        return tables;
    }

    /**
     * Создает sql для создания таблиц динамически из XML
     * @param tableName название таблицы
     * @return String
     */
    public String getTableDDL(String tableName) {
        if (xmlData == null) {
            loadXml();
        }

        switch (tableName.toLowerCase()) {
            case "currency":
                return generateCurrencyDDL();
            case "categories":
                return generateCategoriesDDL();
            case "offers":
                return generateOffersDDL();
            default:
                throw new IllegalArgumentException("Неизвестное имя таблицы: " + tableName);
        }
    }

    private String generateCurrencyDDL() {
        return """
            CREATE TABLE IF NOT EXISTS currency (
                id VARCHAR(10) PRIMARY KEY,
                rate NUMERIC(10, 4) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_currency_id ON currency(id);
            """;
    }

    private String generateCategoriesDDL() {
        return """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL
            );
            CREATE INDEX IF NOT EXISTS idx_categories_parent_id ON categories(parent_id);
            """;
    }

    private String generateOffersDDL() {
        return """
            CREATE TABLE IF NOT EXISTS offers (
                id INTEGER PRIMARY KEY,
                available BOOLEAN NOT NULL,
                url TEXT,
                price NUMERIC(10, 2),
                currency_id VARCHAR(10),
                category_id INTEGER,
                picture TEXT,
                name TEXT NOT NULL,
                vendor TEXT,
                vendor_code VARCHAR(100) UNIQUE NOT NULL,
                description TEXT,
                count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (currency_id) REFERENCES currency(id),
                FOREIGN KEY (category_id) REFERENCES categories(id)
            );
            CREATE INDEX IF NOT EXISTS idx_offers_vendor_code ON offers(vendor_code);
            CREATE INDEX IF NOT EXISTS idx_offers_category_id ON offers(category_id);
            CREATE INDEX IF NOT EXISTS idx_offers_currency_id ON offers(currency_id);
            
            CREATE TABLE IF NOT EXISTS offer_params (
                id SERIAL PRIMARY KEY,
                offer_id INTEGER NOT NULL,
                param_name TEXT NOT NULL,
                param_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (offer_id) REFERENCES offers(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_offer_params_offer_id ON offer_params(offer_id);
            """;
    }

    /**
     * обновляет данные в таблицах бд
     * на основе Id
     * если поменялась структура выдает exception
     */
    public void update() {
        update("currency");
        update("categories");
        update("offers");
    }

    /**
     * обновляет данные в таблицах бд
     * если поменялась структура выдает exception
     * @param tableName название таблицы
     */
    public void update(String tableName) {
        if (xmlData == null) {
            loadXml();
        }

        try {
            connection.setAutoCommit(false);

            switch (tableName.toLowerCase()) {
                case "currency":
                    updateCurrencies();
                    break;
                case "categories":
                    updateCategories();
                    break;
                case "offers":
                    updateOffers();
                    break;
                default:
                    throw new IllegalArgumentException("Неизвестное имя таблицы: " + tableName);
            }

            connection.commit();
            System.out.println("Таблица " + tableName + " успешно обновлена");

        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException("Ошибка отката транзакции", ex);
            }
            throw new RuntimeException("Ошибка обновления таблицы " + tableName + ": " + e.getMessage(), e);
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                // игнорируем
            }
        }
    }

    private void updateCurrencies() throws SQLException {
        // Проверяем структуру таблицы
        checkTableStructure("currency", new String[]{"id", "rate"});

        String upsertSQL = "INSERT INTO currency (id, rate, updated_at) " +
                "VALUES (?, ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (id) DO UPDATE SET " +
                "    rate = EXCLUDED.rate, " +
                "    updated_at = EXCLUDED.updated_at";

        try (PreparedStatement stmt = connection.prepareStatement(upsertSQL)) {
            // Используем Groovy API в Java
            GPathResult shop = (GPathResult) xmlData.getProperty("shop");
            GPathResult currencies = (GPathResult) shop.getProperty("currencies");

            if (currencies != null) {
                Iterable<?> currencyList = (Iterable<?>) currencies.getProperty("currency");

                for (Object currencyObj : currencyList) {
                    NodeChild currency = (NodeChild) currencyObj;

                    String id = currency.attributes().get("id").toString();
                    String rate = currency.attributes().get("rate").toString();

                    stmt.setString(1, id);
                    stmt.setBigDecimal(2, new BigDecimal(rate));
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        } catch (Exception e) {
            throw new SQLException("Ошибка обработки валют: " + e.getMessage(), e);
        }
    }

    private void updateCategories() throws SQLException {
        // Проверяем структуру таблицы
        checkTableStructure("categories", new String[]{"id", "name", "parent_id"});

        String upsertSQL = "INSERT INTO categories (id, name, parent_id, updated_at) " +
                "VALUES (?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (id) DO UPDATE SET " +
                "    name = EXCLUDED.name, " +
                "    parent_id = EXCLUDED.parent_id, " +
                "    updated_at = EXCLUDED.updated_at";

        try (PreparedStatement stmt = connection.prepareStatement(upsertSQL)) {
            GPathResult shop = (GPathResult) xmlData.getProperty("shop");
            GPathResult categories = (GPathResult) shop.getProperty("categories");

            if (categories != null) {
                Iterable<?> categoryList = (Iterable<?>) categories.getProperty("category");

                for (Object categoryObj : categoryList) {
                    NodeChild category = (NodeChild) categoryObj;

                    String id = category.attributes().get("id").toString();
                    String name = category.text().toString();
                    Object parentIdAttr = category.attributes().get("parentId");

                    stmt.setInt(1, Integer.parseInt(id));
                    stmt.setString(2, name);

                    if (parentIdAttr != null) {
                        stmt.setInt(3, Integer.parseInt(parentIdAttr.toString()));
                    } else {
                        stmt.setNull(3, Types.INTEGER);
                    }

                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        } catch (Exception e) {
            throw new SQLException("Ошибка обработки категорий: " + e.getMessage(), e);
        }
    }

    private void updateOffers() throws SQLException {
        // Проверяем структуру таблицы
        checkTableStructure("offers", new String[]{"id", "available", "url", "price",
                "currency_id", "category_id", "picture", "name", "vendor", "vendor_code",
                "description", "count"});

        // Обновляем offers
        String offerUpsertSQL = "INSERT INTO offers (id, available, url, price, currency_id, category_id, " +
                "    picture, name, vendor, vendor_code, description, count, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (id) DO UPDATE SET " +
                "    available = EXCLUDED.available, " +
                "    url = EXCLUDED.url, " +
                "    price = EXCLUDED.price, " +
                "    currency_id = EXCLUDED.currency_id, " +
                "    category_id = EXCLUDED.category_id, " +
                "    picture = EXCLUDED.picture, " +
                "    name = EXCLUDED.name, " +
                "    vendor = EXCLUDED.vendor, " +
                "    vendor_code = EXCLUDED.vendor_code, " +
                "    description = EXCLUDED.description, " +
                "    count = EXCLUDED.count, " +
                "    updated_at = EXCLUDED.updated_at";

        // Для параметров сначала удаляем старые (используем batch)
        String deleteParamsSQL = "DELETE FROM offer_params WHERE offer_id = ?";
        String insertParamSQL = "INSERT INTO offer_params (offer_id, param_name, param_value) " +
                "VALUES (?, ?, ?)";

        try (PreparedStatement offerStmt = connection.prepareStatement(offerUpsertSQL);
             PreparedStatement deleteParamsStmt = connection.prepareStatement(deleteParamsSQL);
             PreparedStatement insertParamStmt = connection.prepareStatement(insertParamSQL)) {

            GPathResult shop = (GPathResult) xmlData.getProperty("shop");
            if (shop == null) {
                System.out.println("Отсутствует элемент shop в XML");
                return;
            }

            GPathResult offers = (GPathResult) shop.getProperty("offers");
            if (offers == null) {
                System.out.println("Отсутствует элемент offers в XML");
                return;
            }

            Object offerProperty = offers.getProperty("offer");
            if (offerProperty == null) {
                System.out.println("Отсутствуют товары (offer) в XML");
                return;
            }

            Iterable<?> offerList;
            if (offerProperty instanceof Iterable) {
                offerList = (Iterable<?>) offerProperty;
            } else {
                // Если только один товар
                offerList = Collections.singletonList(offerProperty);
            }

            List<Integer> processedOfferIds = new ArrayList<>();
            int batchCount = 0;
            final int BATCH_SIZE = 1000;

            for (Object offerObj : offerList) {
                NodeChild offer = (NodeChild) offerObj;

                // Безопасное получение атрибутов
                Map<String, Object> attributes = offer.attributes();
                String idStr = (attributes != null && attributes.containsKey("id")) ?
                        attributes.get("id").toString() : null;
                String availableStr = (attributes != null && attributes.containsKey("available")) ?
                        attributes.get("available").toString() : "false";

                if (idStr == null) {
                    System.out.println("Найден товар без ID, пропускаем");
                    continue;
                }

                int id;
                try {
                    id = Integer.parseInt(idStr);
                } catch (NumberFormatException e) {
                    System.out.println("Некорректный ID товара: " + idStr);
                    continue;
                }

                processedOfferIds.add(id);

                // Обновляем основную информацию о товаре
                offerStmt.setInt(1, id);
                offerStmt.setBoolean(2, Boolean.parseBoolean(availableStr));

                // Используем методы для получения текстовых значений
                setStringOrNull(offerStmt, 3, getChildText(offer, "url"));
                setBigDecimalOrNull(offerStmt, 4, getChildText(offer, "price"));
                setStringOrNull(offerStmt, 5, getChildText(offer, "currencyId"));
                setIntegerOrNull(offerStmt, 6, getChildText(offer, "categoryId"));
                setStringOrNull(offerStmt, 7, getChildText(offer, "picture"));
                setStringOrNull(offerStmt, 8, getChildText(offer, "name"));
                setStringOrNull(offerStmt, 9, getChildText(offer, "vendor"));
                setStringOrNull(offerStmt, 10, getChildText(offer, "vendorCode"));
                setStringOrNull(offerStmt, 11, getChildText(offer, "description"));
                setIntegerOrNull(offerStmt, 12, getChildText(offer, "count"));

                offerStmt.addBatch();
                batchCount++;

                // Добавляем удаление параметров в batch
                deleteParamsStmt.setInt(1, id);
                deleteParamsStmt.addBatch();

                // Добавляем новые параметры
                Object paramProperty = offer.getProperty("param");
                if (paramProperty != null) {
                    Iterable<?> paramList;
                    if (paramProperty instanceof Iterable) {
                        paramList = (Iterable<?>) paramProperty;
                    } else {
                        paramList = Collections.singletonList(paramProperty);
                    }

                    for (Object paramObj : paramList) {
                        NodeChild param = (NodeChild) paramObj;

                        Map<String, Object> paramAttributes = param.attributes();
                        if (paramAttributes != null && paramAttributes.containsKey("name")) {
                            String paramName = paramAttributes.get("name").toString();
                            String paramValue = param.text() != null ? param.text().toString() : "";

                            insertParamStmt.setInt(1, id);
                            insertParamStmt.setString(2, paramName);
                            insertParamStmt.setString(3, paramValue);
                            insertParamStmt.addBatch();
                        }
                    }
                }

                // Выполняем batch при достижении лимита
                if (batchCount >= BATCH_SIZE) {
                    offerStmt.executeBatch();
                    deleteParamsStmt.executeBatch();
                    insertParamStmt.executeBatch();
                    batchCount = 0;
                }
            }

            // Выполняем оставшиеся batch операции
            if (batchCount > 0) {
                offerStmt.executeBatch();
                deleteParamsStmt.executeBatch();
                insertParamStmt.executeBatch();
            }

            System.out.println("Обработано товаров: " + processedOfferIds.size());

        } catch (Exception e) {
            throw new SQLException("Ошибка обработки товаров: " + e.getMessage(), e);
        }
    }

    // Вспомогательные методы для обработки XML
    private String getChildText(NodeChild node, String childName) {
        try {
            Object child = node.getProperty(childName);
            if (child instanceof NodeChild) {
                return ((NodeChild) child).text().toString();
            } else if (child instanceof Iterable) {
                // Если есть несколько элементов, берем первый
                Iterable<?> children = (Iterable<?>) child;
                if (children.iterator().hasNext()) {
                    Object firstChild = children.iterator().next();
                    if (firstChild instanceof NodeChild) {
                        return ((NodeChild) firstChild).text().toString();
                    }
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private void setStringOrNull(PreparedStatement stmt, int index, String value) throws SQLException {
        if (value != null && !value.isEmpty()) {
            stmt.setString(index, value);
        } else {
            stmt.setNull(index, Types.VARCHAR);
        }
    }

    private void setIntegerOrNull(PreparedStatement stmt, int index, String value) throws SQLException {
        if (value != null && !value.isEmpty()) {
            try {
                stmt.setInt(index, Integer.parseInt(value));
            } catch (NumberFormatException e) {
                stmt.setNull(index, Types.INTEGER);
            }
        } else {
            stmt.setNull(index, Types.INTEGER);
        }
    }

    private void setBigDecimalOrNull(PreparedStatement stmt, int index, String value) throws SQLException {
        if (value != null && !value.isEmpty()) {
            try {
                stmt.setBigDecimal(index, new BigDecimal(value));
            } catch (NumberFormatException e) {
                stmt.setNull(index, Types.DECIMAL);
            }
        } else {
            stmt.setNull(index, Types.DECIMAL);
        }
    }

    private void checkTableStructure(String tableName, String[] expectedColumns) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        List<String> actualColumns = new ArrayList<>();

        try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
            while (columns.next()) {
                actualColumns.add(columns.getString("COLUMN_NAME").toLowerCase());
            }
        }

        // Проверяем наличие всех ожидаемых колонок
        for (String expectedColumn : expectedColumns) {
            if (!actualColumns.contains(expectedColumn.toLowerCase())) {
                throw new RuntimeException("Структура таблицы " + tableName + " изменилась. Отсутствует колонка: " + expectedColumn);
            }
        }
    }

    // Функции по желанию

    /**
     * наименование столбцов таблицы (динамически)
     */
    public ArrayList<String> getColumnNames(String tableName) {
        ArrayList<String> columns = new ArrayList<>();

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getColumns(null, null, tableName, null)) {
                while (resultSet.next()) {
                    columns.add(resultSet.getString("COLUMN_NAME"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка получения колонок таблицы " + tableName, e);
        }

        return columns;
    }

    /**
     * true если столбец не имеет повторяющихся значений
     */
    public boolean isColumnId(String tableName, String columnName) {
        String sql = "SELECT COUNT(DISTINCT " + columnName + ") as distinct_count, " +
                "COUNT(" + columnName + ") as total_count " +
                "FROM " + tableName;

        try (PreparedStatement stmt = connection.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                long distinctCount = rs.getLong("distinct_count");
                long totalCount = rs.getLong("total_count");
                return distinctCount == totalCount && totalCount > 0;
            }

            return false;
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка проверки уникальности колонки", e);
        }
    }

    /**
     * изменения таблицы, допустимо только добавление новых столбцов
     */
    public String getDDLChange(String tableName) {
        // Без чёткой спецификации возвращаю заглушку
        return "-- ALTER TABLE " + tableName + " ADD COLUMN new_column_name data_type;";
    }

    /**
     * Закрывает соединение с БД
     */
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Ошибка при закрытии соединения: " + e.getMessage());
            }
        }
    }
}

