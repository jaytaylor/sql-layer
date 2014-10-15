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

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.collation.TestKeyCreator;

import static com.foundationdb.sql.TestBase.*;
import static com.foundationdb.sql.optimizer.OptimizerTestBase.*;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.StringWriter;
import java.util.*;
import java.io.File;

public class IndexStatisticsYamlTest
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + IndexStatisticsYamlTest.class.getPackage().getName().replace('.', '/'));
    public static final File SCHEMA_FILE = new File(RESOURCE_DIR, "schema.ddl");
    public static final File YAML_FILE = new File(RESOURCE_DIR, "stats.yaml");
    
    private AkibanInformationSchema ais;

    @Before
    public void loadSchema() throws Exception {
        ais = parseSchema(SCHEMA_FILE);
    }    

    @Test
    public void testLoadDump() throws Exception {
        IndexStatisticsYamlLoader loader = new IndexStatisticsYamlLoader(ais, "test", new TestKeyCreator());
        Map<Index,IndexStatistics> stats = loader.load(YAML_FILE);
        File tempFile = File.createTempFile("stats", ".yaml");
        tempFile.deleteOnExit();
        StringWriter tempWriter = new StringWriter();
        loader.dump(stats, tempWriter);
        assertEquals("dump matches load", 
                     fileContents(YAML_FILE).replace("\r", ""),
                     tempWriter.toString().replace("\r", ""));
    }

}
