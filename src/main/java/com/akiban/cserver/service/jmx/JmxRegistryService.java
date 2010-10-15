package com.akiban.cserver.service.jmx;

public interface JmxRegistryService {
    void register(JmxManageable service);
    void unregister(JmxManageable service);
}
