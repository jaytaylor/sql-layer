package com.akiban.cserver.service.jmx;

public interface JmxRegistryService extends JmxRegistryServiceMXBean {
    void register(JmxManageable service);
}
