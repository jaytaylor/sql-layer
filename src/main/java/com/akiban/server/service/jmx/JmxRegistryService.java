
package com.akiban.server.service.jmx;

import javax.management.ObjectName;

public interface JmxRegistryService extends JmxRegistryServiceMXBean {
    ObjectName register(JmxManageable service);
    void unregister(ObjectName registeredObject);
}
