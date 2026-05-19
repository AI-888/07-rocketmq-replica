package org.apache.rocketmq.hasync.config;

public class NameServerAddressUtils extends org.apache.rocketmq.common.utils.NameServerAddressUtils {
    public static void setNameServerAddresses(String nameServerAddresses) {
        System.setProperty("rocketmq.namesrv.addr", nameServerAddresses);
    }
}
