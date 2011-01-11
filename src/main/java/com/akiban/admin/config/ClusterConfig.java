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

    public Map<String, ChunkserverNetworkConfig> chunkservers()
    {
        return Collections.unmodifiableMap(chunkservers);
    }

    public ChunkserverNetworkConfig leadChunkserver()
    {
        return leadChunkserver;
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
                ChunkserverNetworkConfig chunkserverNetworkConfig = new ChunkserverNetworkConfig(key, address, lead);
                if (lead) {
                    leadChunkserver = chunkserverNetworkConfig;
                }
                chunkservers.put(key, chunkserverNetworkConfig);
            }
        }
    }

    private static String ADMIN = "$admin";
    private static String MYSQL = "$mysql";
    private static String ZOOKEEPER = "$zookeeper";

    private Address mysql;
    private Address admin;
    private Address zookeeper;
    private final Map<String, ChunkserverNetworkConfig> chunkservers = new HashMap<String, ChunkserverNetworkConfig>();
    private ChunkserverNetworkConfig leadChunkserver;
}
