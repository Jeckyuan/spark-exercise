package com.yuanjk.spark.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author yuanjk
 * @version 22/2/10
 */
public class LoadDataToIgnite {
    public static final Logger log = LoggerFactory.getLogger(LoadDataToIgnite.class);

    public static ClientConfiguration igniteConfig;

    public static String DATA_REGION = "Main_DataStore_Region";

    public static final String TEMPLATE = "PARTITIONED ";

    public static void dataLoader(String schemaName, String tableName, Map<String, Object> dataPropertiesValue) {
        Set<String> dataPropertyNameSet = dataPropertiesValue.keySet();

        String[] dataPropertiesNameArray = dataPropertyNameSet.stream().toArray(n -> new String[n]);
        StringBuffer propertiesNameSb = new StringBuffer();
        StringBuffer propertiesValuePlaceHolderSb = new StringBuffer();
        propertiesNameSb.append("(");
        propertiesValuePlaceHolderSb.append("(");

        Object[] propertiesValueArray = new Object[dataPropertyNameSet.size()];

        for (int i = 0; i < dataPropertiesNameArray.length; i++) {
            String currentDataPropertyName = dataPropertiesNameArray[i];
            // get dataType for property value validate
            //String dataType = slicePropertiesMap.get(currentDataPropertyName.toUpperCase());
            propertiesNameSb.append(currentDataPropertyName);
            propertiesValuePlaceHolderSb.append("?");

            if (i < dataPropertiesNameArray.length - 1) {
                propertiesNameSb.append(",");
                propertiesValuePlaceHolderSb.append(",");
            }
            propertiesValueArray[i] = dataPropertiesValue.get(currentDataPropertyName);
        }
        propertiesNameSb.append(")");
        propertiesValuePlaceHolderSb.append(")");


        String sqlFieldsQuerySQL = "INSERT INTO " + " " + tableName + " " + propertiesNameSb.toString() + " VALUES " + propertiesValuePlaceHolderSb.toString();
        SqlFieldsQuery qry = new SqlFieldsQuery(sqlFieldsQuerySQL);

        try (IgniteClient client = Ignition.startClient(igniteConfig)) {
            client.query(new SqlFieldsQuery(sqlFieldsQuerySQL).setArgs(propertiesValueArray).setSchema(schemaName))
                    .getAll();
        } catch (Exception e) {
            log.error("insert data failed", e);
        }
    }

    public static void dataLoader(IgniteClient client, String schemaName, String tableName,
                                  Map<String, Object> dataPropertiesValue) {
        Set<String> dataPropertyNameSet = dataPropertiesValue.keySet();

        String[] dataPropertiesNameArray = dataPropertyNameSet.stream().toArray(n -> new String[n]);
        StringBuffer propertiesNameSb = new StringBuffer();
        StringBuffer propertiesValuePlaceHolderSb = new StringBuffer();
        propertiesNameSb.append("(");
        propertiesValuePlaceHolderSb.append("(");

        Object[] propertiesValueArray = new Object[dataPropertyNameSet.size()];

        for (int i = 0; i < dataPropertiesNameArray.length; i++) {
            String currentDataPropertyName = dataPropertiesNameArray[i];
            // get dataType for property value validate
            //String dataType = slicePropertiesMap.get(currentDataPropertyName.toUpperCase());
            propertiesNameSb.append(currentDataPropertyName);
            propertiesValuePlaceHolderSb.append("?");

            if (i < dataPropertiesNameArray.length - 1) {
                propertiesNameSb.append(",");
                propertiesValuePlaceHolderSb.append(",");
            }
            propertiesValueArray[i] = dataPropertiesValue.get(currentDataPropertyName);
        }
        propertiesNameSb.append(")");
        propertiesValuePlaceHolderSb.append(")");


        String sqlFieldsQuerySQL = "INSERT INTO " + " " + tableName + " " + propertiesNameSb.toString() + " VALUES "
                + propertiesValuePlaceHolderSb.toString();

        client.query(new SqlFieldsQuery(sqlFieldsQuerySQL).setArgs(propertiesValueArray).setSchema(schemaName))
                .getAll();
    }


    public static void dataQuery(IgniteClient client, String schemaName, String tableName, long limit) {
        String sqlFieldsQuerySQL = "SELECT * " + " FROM " + tableName;

        if (limit > 0) {
            sqlFieldsQuerySQL = sqlFieldsQuerySQL + " LIMIT  " + limit;
        }
        List<String> pids = new ArrayList<>(100 * 10000);
        SqlFieldsQuery sql = new SqlFieldsQuery(sqlFieldsQuerySQL).setSchema(schemaName);
        try (QueryCursor<List<?>> cursor = client.query(sql)) {
            for (List<?> row : cursor) {
                pids.add( row.get(0).toString());
//                System.out.println("------------");
//                row.forEach(System.out::println);
//                System.out.println(row.get(5));;
            }
//                System.out.println("personName=" + row.get(0));
        }
        System.out.println("result size: " + pids.size());
    }

    public static void dataQueryServer(Ignite ignite, String schemaName, String tableName, long limit) {
        String sqlFieldsQuerySQL = "SELECT * " + " FROM " + tableName;

        if (limit > 0) {
            sqlFieldsQuerySQL = sqlFieldsQuerySQL + " LIMIT  " + limit;
        }
        List<String> pids = new ArrayList<>(100 * 10000);
        SqlFieldsQuery sql = new SqlFieldsQuery(sqlFieldsQuerySQL).setSchema(schemaName);
        try (QueryCursor<List<?>> cursor = ignite.cache(tableName).query(sql)) {
            for (List<?> row : cursor) {
                pids.add(row.get(0).toString());
//                System.out.println("------------");
//                row.forEach(System.out::println);
//                System.out.println(row.get(5));;
            }
//                System.out.println("personName=" + row.get(0));
        }
        System.out.println("result size: " + pids.size());
    }

    public static void dataLoaderServer(IgniteCache igniteCache, String schemaName, String tableName,
                                        Map<String, Object> dataPropertiesValue) {
        Set<String> dataPropertyNameSet = dataPropertiesValue.keySet();

        String[] dataPropertiesNameArray = dataPropertyNameSet.stream().toArray(n -> new String[n]);
        StringBuffer propertiesNameSb = new StringBuffer();
        StringBuffer propertiesValuePlaceHolderSb = new StringBuffer();
        propertiesNameSb.append("(");
        propertiesValuePlaceHolderSb.append("(");

        Object[] propertiesValueArray = new Object[dataPropertyNameSet.size()];

        for (int i = 0; i < dataPropertiesNameArray.length; i++) {
            String currentDataPropertyName = dataPropertiesNameArray[i];
            // get dataType for property value validate
            //String dataType = slicePropertiesMap.get(currentDataPropertyName.toUpperCase());
            propertiesNameSb.append(currentDataPropertyName);
            propertiesValuePlaceHolderSb.append("?");

            if (i < dataPropertiesNameArray.length - 1) {
                propertiesNameSb.append(",");
                propertiesValuePlaceHolderSb.append(",");
            }
            propertiesValueArray[i] = dataPropertiesValue.get(currentDataPropertyName);
        }
        propertiesNameSb.append(")");
        propertiesValuePlaceHolderSb.append(")");


        String sqlFieldsQuerySQL = "INSERT INTO " + " " + tableName + " " + propertiesNameSb.toString() + " VALUES "
                + propertiesValuePlaceHolderSb.toString();

        igniteCache.query(new SqlFieldsQuery(sqlFieldsQuerySQL).setArgs(propertiesValueArray).setSchema(schemaName))
                .getAll();
    }


    public static void dataLoaderBatch(IgniteClient client, String schemaName, String tableName,
                                       Map<String, Object> dataPropertiesValue) {
        Set<String> dataPropertyNameSet = dataPropertiesValue.keySet();

        String[] dataPropertiesNameArray = dataPropertyNameSet.stream().toArray(n -> new String[n]);
        StringBuffer propertiesNameSb = new StringBuffer();
        StringBuffer propertiesValuePlaceHolderSb = new StringBuffer();
        propertiesNameSb.append("(");
        propertiesValuePlaceHolderSb.append("(");

        Object[] propertiesValueArray = new Object[dataPropertyNameSet.size()];

        for (int i = 0; i < dataPropertiesNameArray.length; i++) {
            String currentDataPropertyName = dataPropertiesNameArray[i];
            // get dataType for property value validate
            //String dataType = slicePropertiesMap.get(currentDataPropertyName.toUpperCase());
            propertiesNameSb.append(currentDataPropertyName);
            propertiesValuePlaceHolderSb.append("?");

            if (i < dataPropertiesNameArray.length - 1) {
                propertiesNameSb.append(",");
                propertiesValuePlaceHolderSb.append(",");
            }
            propertiesValueArray[i] = dataPropertiesValue.get(currentDataPropertyName);
        }
        propertiesNameSb.append(")");
        propertiesValuePlaceHolderSb.append(")");


        String sqlFieldsQuerySQL = "INSERT INTO " + " " + tableName + " " + propertiesNameSb.toString() + " VALUES "
                + propertiesValuePlaceHolderSb.toString();

        client.query(new SqlFieldsQuery(sqlFieldsQuerySQL).setArgs(propertiesValueArray).setSchema(schemaName))
                .getAll();
    }

    public static void createTable(String schemaName, String tableName,
                                   Map<String, IgniteUtil.DataTypeEnum> columnDefinitions, String... primaryKeys) {
        Iterator<String> pkIterator = columnDefinitions.keySet().iterator();
        if (primaryKeys.length > 0) {
            pkIterator = Arrays.stream(primaryKeys).iterator();
        }
        StringBuilder pkSb = new StringBuilder();
        pkIterator.forEachRemaining(pk -> pkSb.append(pk).append(", "));
        pkSb.replace(pkSb.lastIndexOf(","), pkSb.length(), "");

        StringBuilder columnSb = new StringBuilder();
        columnDefinitions.forEach((k, v) -> columnSb.append(k).append(" ").append(v).append(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(tableName);
        sb.append(" (").append(columnSb).append("PRIMARY KEY (").append(pkSb).append(")").append(")");
        sb.append(" WITH \"")
                .append("CACHE_NAME=").append(tableName)
                .append(",DATA_REGION=").append(DATA_REGION)
                .append(",TEMPLATE=").append(TEMPLATE)
                .append("\"");

        log.info("create table sql [{}]", sb);
        System.out.println("sql===" + sb);

        try (IgniteClient client = Ignition.startClient(igniteConfig)) {
            client.query(new SqlFieldsQuery(sb.toString()).setSchema(schemaName)).getAll();
//            System.out.println("rs size=" + rs.size());
        } catch (Exception e) {
            e.printStackTrace();
            log.error("start ignite client failed", e);
        }
    }

    public static boolean isCacheExists(String cacheName) {
        if (igniteConfig == null) {
            log.error("please set ignite configuration");
            return false;
        }

        try (IgniteClient client = Ignition.startClient(igniteConfig)) {
            ClientCache<?, ?> cache = client.cache(cacheName);
            if (cache == null) {
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            log.error("start client failed", e);
        }
        return false;
    }

    public static void setIgniteConfig(String... hosts) {
        igniteConfig = new ClientConfiguration()
                .setAddresses(hosts)
                .setPartitionAwarenessEnabled(true);
    }


    public static void emptyTable(String cacheName) {
        try (IgniteClient client = Ignition.startClient(igniteConfig)) {
            ClientCache cache = client.cache(cacheName);
            cache.clear();
        } catch (Exception e) {
            log.error("insert data failed", e);
        }
    }


    public static void deleteTable(String cacheName) {
        try (IgniteClient client = Ignition.startClient(igniteConfig)) {
            ClientCache cache = client.cache(cacheName);
            cache.clear();
            client.destroyCache(cacheName);
        } catch (Exception e) {
            log.error("insert data failed", e);
        }
    }
}
