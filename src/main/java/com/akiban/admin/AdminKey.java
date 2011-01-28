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

    public static String[] REQUIRED_KEYS = new String[]{
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
