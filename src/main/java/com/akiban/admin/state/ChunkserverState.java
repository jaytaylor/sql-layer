package com.akiban.admin.state;

import com.akiban.admin.Admin;
import com.akiban.admin.AdminValue;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

public class ChunkserverState
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

    public ChunkserverState(boolean up, boolean lead)
    {
        this.up = up;
        this.lead = lead;
    }

    public ChunkserverState(AdminValue adminValue) throws UnknownHostException
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