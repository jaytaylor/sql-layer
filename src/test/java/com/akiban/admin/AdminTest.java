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

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.akiban.admin.config.ClusterConfig;

public class AdminTest
{
    @Before
    public void setUp() throws Exception
    {
        File configDir = new File(TEST_CONFIG_DIR, DEFAULT_CONFIG);
        System.setProperty(AKIBAN_ADMIN, configDir.getAbsolutePath());
    }

    @AfterClass
    static public void tearDown() throws Exception
    {
        Admin.only().close();
        System.clearProperty(AKIBAN_ADMIN);
    }

    @Test
    public void testMissingConfig() throws IOException
    {
        System.setProperty(AKIBAN_ADMIN, "/no/such/directory");
        try {
            Admin admin = Admin.only();
            Assert.assertTrue(false);
        } catch (Admin.RuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testBadKey() throws IOException
    {
        Admin admin = Admin.only();
        try {
            admin.get("this is not a key");
            Assert.assertTrue(false);
        } catch (Admin.BadKeyException e) {
            // Expected
        }
    }

    @Test
    public void testClusterConfig() throws IOException
    {
        Admin admin = Admin.only();
        ClusterConfig clusterConfig = new ClusterConfig(admin.get("/config/cluster.properties"));
        Assert.assertEquals(1, clusterConfig.chunkservers().size());
        Assert.assertEquals(clusterConfig.chunkservers().get("cs5140"), clusterConfig.leadChunkserver());
        Assert.assertEquals(8765, clusterConfig.admin().port());
        Assert.assertEquals(33060, clusterConfig.mysql().port());
        Assert.assertEquals(2181, clusterConfig.zookeeper().port());
    }

    private static final String AKIBAN_ADMIN = "akiban.admin";
    private static final String DEFAULT_CONFIG = "default";
    private static final String TEST_CONFIG_DIR = "src/test/resources/admin/";
}
