package com.akiban.cserver.service.config;

import java.util.Map;

public interface ConfigurationService
{
    String getProperty(String moduleName, String propertyName);
    String getProperty(String moduleName, String propertyName, String defaultValue);
    Map<String, String> getProperties(String moduleName);
}
