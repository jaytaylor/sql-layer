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

import java.util.Properties;

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

    /**
     * Gets all of the properties for this module as a java.util.Properties object.
     * @return a Properties where each key corresponds to a propertyName (without module name), and each
     * value is its value.
     */
    Properties getProperties();


}
