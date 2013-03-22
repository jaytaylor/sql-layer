
package com.akiban.server.store.statistics;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.server.collation.TestKeyCreator;

import static com.akiban.sql.TestBase.*;
import static com.akiban.sql.optimizer.OptimizerTestBase.*;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

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
        StringWriter tempWriter = new StringWriter();
        loader.dump(stats, tempWriter);
        assertEquals("dump matches load", 
                     fileContents(YAML_FILE).replace("\r", ""),
                     tempWriter.toString().replace("\r", ""));
    }

}
