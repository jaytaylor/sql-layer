/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.config;

public interface ConfigurationService
{
    /**
     * Gets the specified property, or a default if the property isn't set.
     * @param propertyName the property's name
     * @param defaultValue the default value to return, if the given property isn't found
     * @return the property's value, or the given default
     */
    String getProperty(String propertyName, String defaultValue);

    /**
     * Gets the specified property.
     * @param propertyName the property name
     * @return the specified property's value
     * @throws PropertyNotDefinedException if the given module and property are not defined.
     */
    String getProperty(String propertyName) throws PropertyNotDefinedException;

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
