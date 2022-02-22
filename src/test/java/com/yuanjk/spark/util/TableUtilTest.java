package com.yuanjk.spark.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author yuanjk
 * @version 22/2/17
 */
public class TableUtilTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void listCacheNames() {
        TableUtil.listCacheNames();
    }

    @Test
    public void tableMetadata() {
        TableUtil.tableMetadata("road_center_line_99");
    }

    @Test
    public void binaryObject() {
    }
}