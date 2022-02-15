package com.yuanjk.spark.util;

import com.google.gson.Gson;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;

/**
 * @author yuanjk
 * @version 22/2/15
 */
public class TableUtil {

    public static void main(String[] args) {

    }


    public static void listCacheNames() {

        try (Ignite ignite = Ignition.start(IgniteUtil.getIgniteConfiguration(true))) {
            ignite.cacheNames().forEach(System.out::println);
        }
    }

    public static void tableMetadata(String cacheName) {
        try (Ignite ignite = Ignition.start(IgniteUtil.getIgniteConfiguration(true))) {
            IgniteCache igniteCache = ignite.cache(cacheName);
            CacheConfiguration cacheConfiguration = (CacheConfiguration) igniteCache.getConfiguration(
                    CacheConfiguration.class);

            System.out.println("cache configuration type:" + cacheConfiguration.getClass().getName());
            System.out.println("key type:" + cacheConfiguration.getKeyType().getName());
            System.out.println("value type:" + cacheConfiguration.getValueType().getName());

            cacheConfiguration.getKeyConfiguration();

            Gson gson = new Gson();
            Collection<QueryEntity> queryEntities = cacheConfiguration.getQueryEntities();

            for (QueryEntity queryEntity : queryEntities) {
                System.out.println("query entity: " + gson.toJson(queryEntity));
            }

        }
    }

    public static void binaryObject(String cacheName) {
        try (Ignite ignite = Ignition.start(IgniteUtil.getIgniteConfiguration(true))) {
            IgniteCache<Integer, BinaryObject> binaryCache = ignite.cache(cacheName).withKeepBinary();

            BinaryObject binaryObject = binaryCache.get(1);
//binaryObject.
        }
    }

}
