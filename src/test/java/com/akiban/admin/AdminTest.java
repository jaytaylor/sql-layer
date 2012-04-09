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

package com.akiban.admin;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.akiban.admin.config.ClusterConfig;
import com.akiban.server.error.BadAdminDirectoryException;

public class AdminTest
{
    @Before
    public void setUp() throws Exception
    {
        Admin.forget();
        File configDir = new File(TEST_CONFIG_DIR, DEFAULT_CONFIG);
        System.setProperty(AKIBAN_ADMIN, configDir.getAbsolutePath());
    }

    @AfterClass
    static public void tearDown() throws Exception
    {
        Admin.forget();
        System.clearProperty(AKIBAN_ADMIN);
    }

    @Test
    public void testMissingConfig() throws IOException
    {
        System.setProperty(AKIBAN_ADMIN, "/no/such/directory");
        try {
            Admin admin = Admin.only();
            Assert.fail();
        } catch (BadAdminDirectoryException e) {
            // Expected
        }
    }

    @Test
    public void testBadKey() throws IOException
    {
        Admin admin = Admin.only();
        try {
            admin.get("this is not a key");
            Assert.fail();
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
