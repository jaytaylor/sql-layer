package com.akiban.admin.config;

import java.util.Properties;

import com.akiban.admin.AdminValue;

public class ChunkserverConfig
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

    public String decisionEngine()
    {
        return properties.getProperty(PROPERTY_DECISION_ENGINE);
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

    public ChunkserverConfig(AdminValue adminValue)
    {
        this.properties = adminValue.properties();
    }

    private static final String PROPERTY_DATAPATH = "cserver.datapath";
    private static final String PROPERTY_VERBOSE = "cserver.verbose";
    private static final String PROPERTY_DECISION_ENGINE = "cserver.decision_engine";
    private static final String PROPERTY_MYSQL_INSTALL_DIR = "cserver.mysql_install_dir";
    private static final String PROPERTY_JAR_FILE = "cserver.jar_file";
    private static final String PROPERTY_MAX_HEAP_MB = "cserver.max_heap_mb";

    private static final String DEFAULT_MYSQL_INSTALL_DIR = "/usr/local/vanilla";
    private static final String DEFAULT_JAR_FILE = "akiban-cserver-1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static final String DEFAULT_MAX_HEAP_MB = "512";

    private final Properties properties;
}