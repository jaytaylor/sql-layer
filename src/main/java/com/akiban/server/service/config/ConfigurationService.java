/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.config;

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

    long queryTimeoutSec();
    void queryTimeoutSec(long queryTimeoutSec);
    boolean testing();
}
