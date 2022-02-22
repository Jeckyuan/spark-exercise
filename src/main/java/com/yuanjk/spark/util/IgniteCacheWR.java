package com.yuanjk.spark.util;

import com.yuanjk.spark.util.model.Organization;
import com.yuanjk.spark.util.model.Person;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * @author yuanjk
 * @version 22/2/17
 */
public class IgniteCacheWR {

    public static void createCache(IgniteConfiguration configuration, String cacheName) {
        try (Ignite ignite = Ignition.start(configuration)) {
            CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();
            cfg.setName(cacheName);
            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cfg.setDataRegionName("Default_DataStore_Region");

            IgniteCache<Integer, String> cache = ignite.createCache(cfg);
            long startDate = System.currentTimeMillis();
            for (int i = 0; i < 30 * 10000; i++) {
                if (i % 10000 == 9999) {
                    System.out.println(
                            "[" + i + "] is inserted, qps=" + i / (System.currentTimeMillis() - startDate) * 1000);
                }
                cache.put(i, Integer.toString(i));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createCacheStream(IgniteConfiguration configuration, String cacheName, int roundNum) {
        try (Ignite ignite = Ignition.start(configuration)) {
            CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();
            cfg.setName(cacheName);
            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cfg.setDataRegionName("Default_DataStore_Region");

            IgniteCache<Integer, String> cache = ignite.createCache(cfg);
            long startDate = System.currentTimeMillis();


            for (int i = 1; i <= roundNum; i++) {
                if (i % 10 == 9) {
                    System.out.println(
                            "[" + i * 1000 + "] is inserted, qps=" + i * 1000 * 1000 / (System.currentTimeMillis() - startDate));
                }
                Map<Integer, String> data = IntStream.range((i - 1) * 1000, i * 1000).boxed()
                        .collect(Collectors.toMap(m -> m, Object::toString));

                cache.putAll(data);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void createCacheStreamPerson(IgniteConfiguration configuration, String cacheName, int roundNum) {
        try (Ignite ignite = Ignition.start(configuration)) {
            CacheConfiguration<Long, Person> cfg = new CacheConfiguration<>();
            cfg.setName(cacheName);
            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cfg.setDataRegionName("Default_DataStore_Region");
            cfg.setIndexedTypes(Long.class, Person.class);

            IgniteCache<Long, Person> cache = ignite.createCache(cfg);
            long startDate = System.currentTimeMillis();


            for (int i = 1; i <= roundNum; i++) {
                if (i % 10 == 9) {
                    System.out.println(
                            "[" + i * 1000 + "] is inserted, qps=" + (long) i * 1000 * 1000 / (System.currentTimeMillis() - startDate));
                }
                Map<Long, Person> data = LongStream.range((i - 1) * 1000L, i * 1000L).boxed()
                        .collect(Collectors.toMap(m -> m,
                                m -> (new Person(m, m % 100, "first_name_" + m,
                                        "last_name_" + m, Math.random(),
                                        "resume"))));

                cache.putAll(data);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void readCacheStreamPerson(IgniteConfiguration configuration, String cacheName, int roundNum) {
        try (Ignite ignite = Ignition.start(configuration); FileWriter fw = new FileWriter(cacheName)) {
            IgniteCache<Long, Person> cache = ignite.cache(cacheName);
            long startDate = System.currentTimeMillis();

            for (int i = 1; i <= roundNum; i++) {
                if (i % 10 == 9) {
                    System.out.println(
                            "[" + i * 1000 + "] is inserted, qps=" + i * 1000 * 1000L / (System.currentTimeMillis() - startDate));
                }

                Set<Long> integerSet = LongStream.range((i - 1L) * 1000, i * 1000L).boxed().collect(Collectors.toSet());
                cache.getAll(integerSet).forEach((k, v) -> {
                    try {
                        fw.write(k + "\t" + v.toString() + "\n");
                    } catch (IOException e) {
                        System.out.println("write to file failed");
                        e.printStackTrace();
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void readCache(IgniteConfiguration configuration, String cacheName) {
        try (Ignite ignite = Ignition.start(configuration)) {

            IgniteCache<Integer, String> cache = ignite.cache(cacheName);
            long startDate = System.currentTimeMillis();
            List<String> stringList = new ArrayList<>(10 * 10000);
            for (int i = 0; i < 30 * 10000; i++) {
                if (i % 10000 == 9999) {
                    System.out.println(
                            "[" + i + "] is read, qps=" + i / (System.currentTimeMillis() - startDate) * 1000);
                }
//                cache.put(i, Integer.toString(i));
                stringList.add(cache.get(i));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void readCacheBatch(IgniteConfiguration configuration, String cacheName, int roundNum) {
        try (Ignite ignite = Ignition.start(configuration)) {
            IgniteCache<Integer, String> cache = ignite.cache(cacheName);
            long startDate = System.currentTimeMillis();

            Map<Integer, String> rs = new HashMap<>(10 * 10000);

            for (int i = 1; i <= roundNum; i++) {
                if (i % 10 == 9) {
                    System.out.println(
                            "[" + i * 1000 + "] is inserted, qps=" + i * 1000 * 1000 / (System.currentTimeMillis() - startDate));
                }

                Set<Integer> integerSet = IntStream.range((i - 1) * 1000, i * 1000).boxed().collect(Collectors.toSet());
                rs.putAll(cache.getAll(integerSet));
            }
            System.out.println("===get result size: " + rs.size());
            System.out.println("===get result size: " + rs.get(100 * 10000 - 1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void deleteCache(IgniteConfiguration configuration, String cacheName) {
        try (Ignite ignite = Ignition.start(configuration)) {
            ignite.destroyCache(cacheName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void cacheValueType(ClientConfiguration clientConfiguration, String cacheName) {

        try (IgniteClient client = Ignition.startClient(clientConfiguration)) {

            ClientCache cache = client.getOrCreateCache(cacheName);

            System.out.println("---start to insert cache---");
            cache.put(1, 1);
            cache.put(2, 'a');
            cache.put(3, "abc");
            cache.put("3", "abc_String");
            cache.put("abc", "abc___");
            cache.put(1L, "1___long");
            cache.put('a', "a___char");
            cache.put("date", new Date());

            System.out.println("---complete insert cache---");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static void queryCacheWithTypes(ClientConfiguration clientConfiguration, String cacheName) {

        try (IgniteClient client = Ignition.startClient(clientConfiguration)) {

            ClientCache cache = client.cache(cacheName);

            System.out.println("---start to query cache---");
            IntStream.rangeClosed(1, 3).forEach(i -> {
                System.out.print("key=" + i);
                System.out.print(", value=" + cache.get(i));
                System.out.println(", value type=" + cache.get(i).getClass().getTypeName());
            });

            String key1 = "3";
            System.out.print("key=" + key1);
            System.out.print(", value=" + cache.get(key1));
            System.out.println(", value type=" + cache.get(key1).getClass().getTypeName());


            String key2 = "abc";
            System.out.print("key=" + key2);
            System.out.print(", value=" + cache.get(key2));
            System.out.println(", value type=" + cache.get(key2).getClass().getTypeName());


            Long key3 = 1L;
            System.out.print("key=" + key3);
            System.out.print(", value=" + cache.get(key3));
            System.out.println(", value type=" + cache.get(key3).getClass().getTypeName());

            char key4 = 'a';
            System.out.print("key=" + key4);
            System.out.print(", value=" + cache.get(key4));
            System.out.println(", value type=" + cache.get(key4).getClass().getTypeName());


            String key5 = "date";
            System.out.print("key=" + key5);
            System.out.print(", value=" + cache.get(key5));
            System.out.println(", value type=" + cache.get(key5).getClass().getTypeName());


        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    public static void createQueryCache(IgniteConfiguration configuration, String cacheName) {
        Ignite ignite = Ignition.start(configuration);
        CacheConfiguration<Long, Organization> personCacheCfg = new CacheConfiguration<>();
        personCacheCfg.setName(cacheName);

        personCacheCfg.setIndexedTypes(Long.class, Organization.class);
        IgniteCache<Long, Organization> cache = ignite.createCache(personCacheCfg);

        Organization organization1 = new Organization("zhansan");
        organization1.setCode("code-01");
        Organization organization2 = new Organization("wangwu");
        organization2.setCode("code-02");
        Organization organization3 = new Organization("zhaoliu");

        cache.put(1L, organization1);
        cache.put(2L, organization2);
        cache.put(3L, organization3);
        ignite.close();
    }

    public static void getQueryCache(IgniteConfiguration configuration) {
        Ignite ignite = Ignition.start(configuration);
        IgniteCache<Long, Organization> cache = ignite.cache("Organization");

        SqlFieldsQuery sql = new SqlFieldsQuery("select name from Person");

        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            for (List<?> row : cursor)
                System.out.println("name=" + row.get(0));
        }

        ignite.close();
    }


}
