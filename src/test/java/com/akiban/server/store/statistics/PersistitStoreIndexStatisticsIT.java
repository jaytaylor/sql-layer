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

package com.akiban.server.store.statistics;

import com.akiban.sql.pg.PostgresServerFilesITBase;
import static com.akiban.sql.TestBase.*;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.util.*;
import java.io.File;

public class PersistitStoreIndexStatisticsIT extends PostgresServerFilesITBase
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + IndexStatisticsYamlTest.class.getPackage().getName().replace('.', '/'));
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

    @Override
    public void openTheConnection() throws Exception {
        // TODO: Test class modularity isn't right.
    }
    
    @Test
    public void testLoadDump() throws Exception {
        service.loadIndexStatistics(session(), SCHEMA_NAME, YAML_FILE);
        service.clearCache();
        File tempFile = File.createTempFile("stats", ".yaml");
        service.dumpIndexStatistics(session(), SCHEMA_NAME, tempFile);
        assertEquals("dump matches load", 
                     fileContents(YAML_FILE).replace("\r", ""),
                     fileContents(tempFile).replace("\r", ""));
    }

}
