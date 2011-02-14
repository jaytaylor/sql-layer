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

package com.akiban.admin;

import java.io.IOException;
import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

public class AkServerState
{
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

    AkServerState(String name, String config) throws UnknownHostException
    {
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(config));
        } catch (IOException e) {
            throw new Admin.RuntimeException(String.format("Unable to load chunkserver state: %s", config));
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.equals(STATE)) {
                up = value.equals("up");
            } else if (key.equals(LEAD)) {
                lead = value.equals("true");
            } else {
                throw new Admin.RuntimeException
                    (String.format("Unsupported property for chunkserver state %s: %s", name, key));
            }
        }
    }

    private static String STATE = "state";
    private static String LEAD = "lead";

    private boolean up = false;
    private boolean lead = false;
}