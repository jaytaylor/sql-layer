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

package com.akiban.admin.state;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import com.akiban.admin.Admin;
import com.akiban.admin.AdminValue;

public class AkServerState
{
    public int version()
    {
        return version;
    }

    public boolean up()
    {
        return up;
    }

    public void up(boolean x)
    {
        up = x;
    }

    public boolean lead()
    {
        return lead;
    }

    public String toPropertiesString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(String.format("state = %s\n", up ? "up" : "down"));
        buffer.append(String.format("lead = %s\n", lead));
        return buffer.toString();
    }

    public AkServerState(boolean up, boolean lead)
    {
        this.up = up;
        this.lead = lead;
    }

    public AkServerState(AdminValue adminValue) throws UnknownHostException
    {
        this.version = adminValue.version();
        Properties properties = adminValue.properties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.equals(STATE)) {
                up = value.equals("up");
            } else if (key.equals(LEAD)) {
                lead = value.equals("true");
            } else {
                throw new Admin.RuntimeException
                    (String.format("Unsupported chunkserver property: %s", key));
            }
        }
    }

    private static String STATE = "state";
    private static String LEAD = "lead";

    private int version;
    private boolean up = false;
    private boolean lead = false;
}