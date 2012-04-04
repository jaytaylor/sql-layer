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

package com.akiban.admin.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.akiban.admin.Address;
import com.akiban.admin.AdminValue;

public class ClusterConfig
{
    public Address mysql()
    {
        return mysql;
    }

    public Address admin()
    {
        return admin;
    }

    public Address zookeeper()
    {
        return zookeeper;
    }

    public Map<String, AkServerNetworkConfig> chunkservers()
    {
        return Collections.unmodifiableMap(chunkservers);
    }

    public AkServerNetworkConfig leadChunkserver()
    {
        return leadAkServer;
    }

    public ClusterConfig(AdminValue adminValue) 
    {
        for (Map.Entry<Object, Object> entry : adminValue.properties().entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.equals(ADMIN)) {
                admin = new Address(value);
            } else if (key.equals(MYSQL)) {
                mysql = new Address(value);
            } else if (key.equals(ZOOKEEPER)) {
                zookeeper = new Address(value);
            } else {
                Address address;
                boolean lead = value.charAt(0) == '*';
                address = lead ? new Address(value.substring(1).trim()) : new Address(value);
                AkServerNetworkConfig akServerNetworkConfig = new AkServerNetworkConfig(key, address, lead);
                if (lead) {
                    leadAkServer = akServerNetworkConfig;
                }
                chunkservers.put(key, akServerNetworkConfig);
            }
        }
    }

    private static String ADMIN = "$admin";
    private static String MYSQL = "$mysql";
    private static String ZOOKEEPER = "$zookeeper";

    private Address mysql;
    private Address admin;
    private Address zookeeper;
    private final Map<String, AkServerNetworkConfig> chunkservers = new HashMap<String, AkServerNetworkConfig>();
    private AkServerNetworkConfig leadAkServer;
}
