package com.akiban.cserver.service.config;

import com.akiban.cserver.service.MxService;

public interface ConfigurationServiceMXBean extends MxService
{
    String getProperty(String moduleName, String propertyName);
}
