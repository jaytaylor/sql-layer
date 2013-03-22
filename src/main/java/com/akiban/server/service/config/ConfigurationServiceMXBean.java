
package com.akiban.server.service.config;

import java.util.Map;
import java.util.Set;

public interface ConfigurationServiceMXBean
{
    Map<String,String> getProperties();

    long getQueryTimeoutMilli();

    void setQueryTimeoutMilli(long timeoutMilli);
}
