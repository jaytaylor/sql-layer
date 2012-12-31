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
