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

import com.akiban.admin.Address;

// Represents chunkserver address, port & lead config, specified in /config/cluster.properties
// For configuration details of a single chunkserver, the class to use is ChunkserverConfig

public class AkServerNetworkConfig
{
    @Override
    public String toString()
    {
        return String.format("Chunkserver(%s%s)", lead ? "*" : "", address);
    }

    @Override
    public boolean equals(Object o)
    {
        AkServerNetworkConfig that = (AkServerNetworkConfig) o;
        return this.name.equals(that.name) &&
               this.address.equals(that.address) &&
               this.lead == that.lead;
    }

    public AkServerNetworkConfig(String name, Address address, boolean lead)
    {
        this.name = name;
        this.address = address;
        this.lead = lead;
    }

    public String name()
    {
        return name;
    }

    public Address address()
    {
        return address;
    }

    public boolean lead()
    {
        return lead;
    }

    private final String name;
    private final Address address;
    private boolean lead;
}
