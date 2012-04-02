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

import java.util.Properties;

import com.akiban.admin.AdminValue;

public class AkServerConfig
{
    public String datapath()
    {
        return properties.getProperty(PROPERTY_DATAPATH);
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

    private static final String PROPERTY_DATAPATH = "akserver.datapath";
    private static final String PROPERTY_MYSQL_INSTALL_DIR = "akserver.mysql_install_dir";
    private static final String PROPERTY_JAR_FILE = "akserver.jar_file";
    private static final String PROPERTY_MAX_HEAP_MB = "akserver.max_heap_mb";

    private static final String DEFAULT_MYSQL_INSTALL_DIR = "/usr/local/vanilla";
    private static final String DEFAULT_JAR_FILE = "akiban-server-1.0-SNAPSHOT-jar-with-dependencies.jar";
    private static final String DEFAULT_MAX_HEAP_MB = "512";

    private final Properties properties;
}
