package com.yuanjk.spark.util;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

/**
 * @author yuanjk
 * @version 22/2/10
 */
public class IgniteUtil {
    public enum DataTypeEnum {
        BOOLEAN, INT, SHORT, BIGINT, FLOAT, DOUBLE, TIMESTAMP, VARCHAR, TINYINT, DECIMAL, BINARY, GEOMETRY, UUID
    }


    public static IgniteConfiguration getIgniteConfiguration(boolean isClientModel) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(isClientModel);
        cfg.setIgniteInstanceName("cim-analysis-ignite-client");

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setLocalAddress("10.2.112.68");
        discoverySpi.setLocalPort(43500);
        TcpDiscoveryVmIpFinder vmIpFinder = new TcpDiscoveryVmIpFinder();

        discoverySpi.setIpFinder(vmIpFinder);
//        String[] addressArr = {"10.0.168.52:43500", "10.0.168.19:43500", "10.0.168.45:43500"};

        String[] addressArr = {"10.2.112.43:43500"};
        vmIpFinder.setAddresses(Arrays.asList(addressArr));

        cfg.setDiscoverySpi(discoverySpi);

        TcpCommunicationSpi communicationSpi = new TcpCommunicationSpi();
        communicationSpi.setLocalPort(43100);
        cfg.setCommunicationSpi(communicationSpi);

        return cfg;

    }
}
