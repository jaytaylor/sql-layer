package com.akiban.cserver.service.config;

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
     * @return the specified property's value
     * @throws PropertyNotDefinedException if the given module and property are not defined.
     */
    String getProperty(String module, String propertyName) throws PropertyNotDefinedException;

    /**
     * Gets a ModuleConfiguration for the specified module. This ModuleConfiguration represents a view
     * of its ConfigurationService; if either is mutable, changes to the one should be reflected in the other.
     *
     * <p>This method should always return a ModuleConfiguration, even if the service isn't started, or there
     * are no properties defined for the given module, or in any other situation. In those cases, the exception
     * (PropertyNotDefinedException, ServiceNotStartedException, etc.) should be thrown when using the
     * ModuleConfiguration instance, not when getting it via this method. This underscores the fact that the
     * ModuleConfiguration is nothing more than a stateless view into this ConfigurationService.</p>
     * @param module the namespace to use
     * @return a ModuleConfiguration backed by this ConfigurationService.
     */
    ModuleConfiguration getModuleConfiguration(String module);
}
