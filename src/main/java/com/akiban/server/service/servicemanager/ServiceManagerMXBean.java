
package com.akiban.server.service.servicemanager;

import java.util.List;

@SuppressWarnings("unused") // jmx
public interface ServiceManagerMXBean {
    public boolean isFullClassNames();
    public void setFullClassNames(boolean value);

    public List<String> getStartedDependencies();
    public void graphStartedDependencies(String filename);
    public List<String> getServicesInStartupOrder();
}
