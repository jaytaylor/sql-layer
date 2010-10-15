package com.akiban.cserver.service.config;

import java.util.Map;
import java.util.Set;

public interface ConfigurationService
{
    public final static String NEWLINE = System.getProperty("line.separator");
    
    /**
     * Gets the specified property.
     * @param module the property's namespace
     * @param propertyName the property name
     * @return the specified property's value, or defaultValue
     * @throws PropertyNotDefinedException
     */
    String getProperty(String module, String propertyName) throws PropertyNotDefinedException;

    /**
     * Gets a mapping of all of the specified namespace's properties. The returned Map must be unmodifiable
     * but thread-safe.
     * @param module the namespace
     * @return an unmodifiable, thread-safe map
     */
    Map<String, Property> getProperties(String module);

    /**
     * Lists all known properties. The returned set should be unmodifiable.
     * @return the unmodifiable set of all set properties.
     */
    Set<Property> getProperties();
}
