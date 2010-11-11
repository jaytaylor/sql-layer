package com.akiban.admin;

import com.akiban.admin.config.ClusterConfig;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.junit.AfterClass;

import java.io.File;
import java.io.IOException;

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
