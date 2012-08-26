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

package com.akiban.admin;

public class AdminKey
{
    // Cluster configuration. Format:
    // name: [*]host:port
    // ...
    // The names $admin and $mysql are reserved for configuring the addresses of the admin service and the mysql head.
    // All other names are chunkserver names. * is used to denote the lead chunkserver. It must be specified for
    // exactly one chunkserver, and never for $admin or $mysql.
    public static final String CONFIG_CLUSTER = "/config/cluster.properties";

    // Chunkserver properties. Format specified by chunkserver
    public static final String CONFIG_CHUNKSERVER = "/config/server.properties";

    // Logging properties. log4j format.
    public static final String CONFIG_LOGGING = "/config/logging.properties";

    public static final String STATE_BASE = "/state";
    public static final String CONFIG_BASE = "/config";

    // Chunkserver status. Format:
    // state: up/down
    // lead: true/false
    public static final String STATE_CHUNKSERVER = "/state/%s.properties";

    public static final String[] REQUIRED_KEYS = new String[]{
        CONFIG_CLUSTER,
        CONFIG_CHUNKSERVER,
        CONFIG_LOGGING
    };

    public static String[] CONFIG_KEYS = new String[]{
        CONFIG_CLUSTER,
        CONFIG_CHUNKSERVER,
        CONFIG_LOGGING
    };

    public static String stateChunkserverName(String chunkserverName)
    {
        return String.format(STATE_CHUNKSERVER, chunkserverName);
    }
}
