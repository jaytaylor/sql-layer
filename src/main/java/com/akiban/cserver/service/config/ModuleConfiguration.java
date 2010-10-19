package com.akiban.cserver.service.config;

/**
 * A module-specific view of a ConfigurationService. If your class always gets properties from just one module,
 * this interface may be more convenient and concise to use.
 */
public interface ModuleConfiguration {

    /**
     * Gets the specified property for this module, or a default if the property isn't set.
     * @param propertyName the property's name
     * @param defaultValue the default value to return, if the given property isn't found
     * @return the property's value, or the given default
     */
    String getProperty(String propertyName, String defaultValue);

    /**
     * Gets the specified property for this module.
     * @param propertyName the property name
     * @return the specified property's value, or defaultValue
     * @throws PropertyNotDefinedException
     */
    String getProperty(String propertyName) throws PropertyNotDefinedException;
}
