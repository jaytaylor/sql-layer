/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.config;

import java.util.Map;
import java.util.Properties;

public interface ConfigurationService
{
    /**
     * Gets the specified property.
     * @param propertyName the property name
     * @return the specified property's value
     * @throws PropertyNotDefinedException if the given module and property are not defined.
     */
    String getProperty(String propertyName) throws PropertyNotDefinedException;

    /**
     * <p>Creates a {@code java.util.Properties} file that reflects all known properties whose keys start with the given
     * prefix. That prefix will be omitted from the keys of the resulting Properties.</p>
     * <p>For instance, if this ConfigurationService had defined properties:
     * <table border="1">
     *     <tr>
     *         <th>key</th>
     *         <th>value</th>
     *     </tr>
     *     <tr>
     *         <td>a.one</td>
     *         <td>1</td>
     *     </tr>
     *     <tr>
     *         <td>a.one.alpha</td>
     *         <td>1a</td>
     *     </tr>
     *     <tr>
     *         <td>a.two</td>
     *         <td>2</td>
     *     </tr>
     *     <tr>
     *         <td>b.three</td>
     *         <td>3</td>
     *     </tr>
     * </table>
     * ...then {@code deriveProperties(a.)} would result in a Properties instance with key-value pairs:
     * <table border="1">
     *     <tr>
     *         <th>key</th>
     *         <th>value</th>
     *     </tr>
     *     <tr>
     *         <td>one</td>
     *         <td>1</td>
     *     </tr>
     *     <tr>
     *         <td>one.alpha</td>
     *         <td>1a</td>
     *     </tr>
     *     <tr>
     *         <td>two</td>
     *         <td>2</td>
     *     </tr>
     * </table>
     * </p>
     * @param withPrefix the key prefix which acts as both a selector and eliding force of keys
     * @return the derived Properties instance, which may be safely altered
     * @throws NullPointerException if withPrefix is null
     */
    Properties deriveProperties(String withPrefix);

    /**
     * Get all of the defined properties as an immutable Map.
     * @return a Map of all defined properties
     */
    Map<String,String> getProperties();

    long queryTimeoutMilli();
    void queryTimeoutMilli(long queryTimeoutMilli);
    boolean testing();
}
