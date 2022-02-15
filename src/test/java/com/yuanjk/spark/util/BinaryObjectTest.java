package com.yuanjk.spark.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yuanjk
 * @version 22/2/15
 */
public class BinaryObjectTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void main() {
    }

    @Test
    public void listCacheNames() {

        TableUtil.listCacheNames();

    }

    @Test
    public  void binaryObject() {
        TableUtil.tableMetadata("road_center_line_99");
//        BinaryObject.binaryObject("CIM_STUDY");
    }
}