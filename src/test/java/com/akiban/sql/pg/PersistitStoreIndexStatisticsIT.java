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

package com.akiban.sql.pg;

import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.store.statistics.IndexStatisticsYamlTest;
import static com.akiban.sql.TestBase.*;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.io.StringWriter;
import java.io.File;
import java.util.concurrent.Callable;

public class PersistitStoreIndexStatisticsIT extends PostgresServerFilesITBase
{
    public static final File RESOURCE_DIR = IndexStatisticsYamlTest.RESOURCE_DIR;
    public static final File YAML_FILE = new File(RESOURCE_DIR, "stats.yaml");
    
    private IndexStatisticsService service;

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @Before
    public void getService() throws Exception {
        service = serviceManager().getServiceByClass(IndexStatisticsService.class);
    }
    
    @Test
    public void testLoadDump() throws Exception {
        transactionallyUnchecked(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                loadDump();
                return null;
            }
        });
    }

    public void loadDump() throws Exception {
        service.loadIndexStatistics(session(), SCHEMA_NAME, YAML_FILE);
        service.clearCache();
        File tempFile = File.createTempFile("stats", ".yaml");
        StringWriter tempWriter = new StringWriter();
        service.dumpIndexStatistics(session(), SCHEMA_NAME, tempWriter);
        assertEquals("dump matches load", 
                     fileContents(YAML_FILE).replace("\r", ""),
                tempWriter.toString().replace("\r", ""));
    }

}
