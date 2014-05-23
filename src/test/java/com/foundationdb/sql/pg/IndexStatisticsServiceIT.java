/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.pg;

import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.store.statistics.IndexStatisticsYamlTest;
import static com.foundationdb.sql.TestBase.*;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.io.StringWriter;
import java.io.File;
import java.util.concurrent.Callable;

public class IndexStatisticsServiceIT extends PostgresServerFilesITBase
{
    public static final File DB_DIR = IndexStatisticsLifecycleIT.RESOURCE_DIR;
    public static final File YAML_FILE = new File(IndexStatisticsYamlTest.RESOURCE_DIR, "stats.yaml");
    
    private IndexStatisticsService service;

    @Before
    public void loadDatabase() throws Exception {
        loadDatabase(DB_DIR);
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
        tempFile.deleteOnExit();
        StringWriter tempWriter = new StringWriter();
        service.dumpIndexStatistics(session(), SCHEMA_NAME, tempWriter);
        assertEquals("dump matches load", 
                     fileContents(YAML_FILE).replace("\r", ""),
                tempWriter.toString().replace("\r", ""));
    }

}
