package com.akiban.cserver.service.config;

import java.util.Set;

public interface ConfigurationService
{
    public final static String NEWLINE = System.getProperty("line.separator");

    /**
     * Gets the specified property, or a default if the property isn't set.
     * @param module the property's module namespace
     * @param propertyName the property's name
     * @param defaultValue the default value to return, if the given property isn't found
     * @return the property's value, or the given default
     */
    String getProperty(String module, String propertyName, String defaultValue);

    /**
     * Gets the specified property.
     * @param module the property's namespace
     * @param propertyName the property name
     * @return the specified property's value, or defaultValue
     * @throws PropertyNotDefinedException
     */
    String getProperty(String module, String propertyName) throws PropertyNotDefinedException;

    /**
     * Lists all known properties. The returned set should be unmodifiable.
     * @return the unmodifiable set of all set properties.
     */
    Set<Property> getProperties();
}
