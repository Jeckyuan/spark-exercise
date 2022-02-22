package com.yuanjk.spark.util;

import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yuanjk
 * @version 22/2/17
 */
public class IgniteCacheWRTest {

    @Before
    public void setUp() throws Exception {

        LoadDataToIgnite.setIgniteConfig("10.2.112.43:10800");

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void createCache() {

        String cacheName = "Performance_Test_Cache12";
        long startDate = System.currentTimeMillis();

        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.createCache(configuration, cacheName);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }


    @Test
    public void createCacheStream() {

        String cacheName = "Performance_Test_Cache";
        long startDate = System.currentTimeMillis();

        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.createCacheStream(configuration, cacheName, 3000);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }


    @Test
    public void createCacheStreamPerson() {

        String cacheName = "ObjectCacheV2";
        long startDate = System.currentTimeMillis();

        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.createCacheStreamPerson(configuration, cacheName, 3000);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }

    @Test
    public void readCacheStreamPerson() {

        String cacheName = "ObjectCacheV2";
        long startDate = System.currentTimeMillis();

        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.readCacheStreamPerson(configuration, cacheName, 3000);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }


    @Test
    public void readCache() {

        String cacheName = "Performance_Test_Cache12";
        long startDate = System.currentTimeMillis();

        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.readCache(configuration, cacheName);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }

    @Test
    public void readCacheBatch() {

        String cacheName = "Performance_Test_Cache";
        long startDate = System.currentTimeMillis();

        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.readCacheBatch(configuration, cacheName, 3000);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }

    @Test
    public void deleteCache() {

//        String cacheName = "Performance_Test_Cache";
//        String cacheName = "Organization";
//        String cacheName = "Object_Performance_Test_Cache_v2";
        String cacheName = "Object_Performance_Test_Cache";
        long startDate = System.currentTimeMillis();

        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.deleteCache(configuration, cacheName);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }


    @Test
    public void cacheValueType() {

        String cacheName = "test_cache_value_type";
        long startDate = System.currentTimeMillis();

        ClientConfiguration clientConfiguration = LoadDataToIgnite.igniteConfig;

        IgniteCacheWR.cacheValueType(clientConfiguration, cacheName);

        IgniteCacheWR.queryCacheWithTypes(clientConfiguration, cacheName);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }


    @Test
    public void createQueryCache() {
        long startDate = System.currentTimeMillis();

        String cacheName = "organization_v2";
        IgniteConfiguration configuration = IgniteUtil.getIgniteConfiguration(true);

        IgniteCacheWR.createQueryCache(configuration, cacheName);

        System.out.println("===used million seconds: " + (System.currentTimeMillis() - startDate));

    }

}