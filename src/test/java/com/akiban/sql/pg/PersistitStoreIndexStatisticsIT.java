
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
