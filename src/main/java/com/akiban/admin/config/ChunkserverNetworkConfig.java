package com.akiban.admin.config;

import com.akiban.admin.Address;

// Represents chunkserver address, port & lead config, specified in /config/cluster.properties
// For configuration details of a single chunkserver, the class to use is ChunkserverConfig

public class ChunkserverNetworkConfig
{
    @Override
    public String toString()
    {
        return String.format("Chunkserver(%s%s)", lead ? "*" : "", address);
    }

    @Override
    public boolean equals(Object o)
    {
        ChunkserverNetworkConfig that = (ChunkserverNetworkConfig) o;
        return this.name.equals(that.name) &&
               this.address.equals(that.address) &&
               this.lead == that.lead;
    }

    public ChunkserverNetworkConfig(String name, Address address, boolean lead)
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
