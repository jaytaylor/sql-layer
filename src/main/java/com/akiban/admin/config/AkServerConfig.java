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

package com.akiban.admin.config;

import java.util.Properties;

import com.akiban.admin.AdminValue;

public class AkServerConfig
{
    public String datapath()
    {
        return properties.getProperty(PROPERTY_DATAPATH);
    }

    public Boolean verbose()
    {
        String verboseString = properties.getProperty(PROPERTY_VERBOSE);
        return verboseString == null ? null : Boolean.valueOf(verboseString);
    }

    public String mysqlInstallDir()
    {
        return properties.getProperty(PROPERTY_MYSQL_INSTALL_DIR, DEFAULT_MYSQL_INSTALL_DIR);
    }

    public String jarFile()
    {
        return properties.getProperty(PROPERTY_JAR_FILE, DEFAULT_JAR_FILE);
    }

    public Integer maxHeapMB()
    {
        return Integer.valueOf(properties.getProperty(PROPERTY_MAX_HEAP_MB, DEFAULT_MAX_HEAP_MB));
    }

    public AkServerConfig(AdminValue adminValue)
    {
        this.properties = adminValue.properties();
    }

    private static final String PROPERTY_DATAPATH = "cserver.datapath";
    private static final String PROPERTY_VERBOSE = "cserver.verbose";
    private static final String PROPERTY_MYSQL_INSTALL_DIR = "cserver.mysql_install_dir";
    private static final String PROPERTY_JAR_FILE = "cserver.jar_file";
    private static final String PROPERTY_MAX_HEAP_MB = "cserver.max_heap_mb";

    private static final String DEFAULT_MYSQL_INSTALL_DIR = "/usr/local/vanilla";
    private static final String DEFAULT_JAR_FILE = "akiban-server-1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static final String DEFAULT_MAX_HEAP_MB = "512";

    private final Properties properties;
}
