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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;

import static com.akiban.sql.TestBase.*;
import static com.akiban.sql.optimizer.OptimizerTestBase.*;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.util.*;
import java.io.File;

public class IndexStatisticsYamlTest
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + IndexStatisticsYamlTest.class.getPackage().getName().replace('.', '/'));
    public static final File SCHEMA_FILE = new File(RESOURCE_DIR, "schema.ddl");
    public static final File INDEX_FILE = new File(RESOURCE_DIR, "group.idx");
    public static final File YAML_FILE = new File(RESOURCE_DIR, "stats.yaml");
    
    private AkibanInformationSchema ais;

    @Before
    public void loadSchema() throws Exception {
        ais = parseSchema(SCHEMA_FILE);
        loadGroupIndexes(ais, INDEX_FILE);
    }    

    @Test
    public void testLoadDump() throws Exception {
        IndexStatisticsYamlLoader loader = new IndexStatisticsYamlLoader(ais, "user");
        Map<Index,IndexStatistics> stats = loader.load(YAML_FILE);
        File tempFile = File.createTempFile("stats", ".yaml");
        loader.dump(stats, tempFile);
        assertEquals("dump matches load", 
                     fileContents(YAML_FILE).replace("\r", ""),
                     fileContents(tempFile).replace("\r", ""));
    }

}
