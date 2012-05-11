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

package com.akiban.sql.optimizer;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class DistinctEliminatorSimpleTest extends DistinctEliminatorTestBase {

    private static final File SIMPLE_TEST = new File(RESOURCE_DIR, "simple-distincts.yaml");

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        FileReader fileReader = new FileReader(SIMPLE_TEST);
        try {
            ParameterizationBuilder pb = new ParameterizationBuilder();
            BufferedReader buffered = new BufferedReader(fileReader);
            Yaml yaml = new Yaml();
            for (Object objRaw : yaml.loadAll(buffered)) {
                List<?> asList = (List<?>) objRaw;
                for (Object lineRaw : asList) {
                    Map<?,?> line = (Map<?,?>) lineRaw;
                    if (line.size() != 1)
                        throw new RuntimeException("need key-val pair:" + line);
                    String actionStr = (String) line.keySet().iterator().next();
                    String sql = (String) line.get(actionStr);

                    String name = sql;
                    if (name.startsWith("SELECT DISTINCT"))
                        name = name.substring("SELECT DISTINCT".length());

                    KeepOrOptimize action = KeepOrOptimize.valueOf(actionStr.toUpperCase());
                    pb.create(name, action != KeepOrOptimize.IGNORED, sql,  action);
                }
            }
            return pb.asList();
        }
        finally {
            fileReader.close();
        }
    }

    @Test
    public void test() throws Exception {
        if (!sql.toUpperCase().contains("DISTINCT"))
            throw new RuntimeException("original didn't have DISTINCT");
        String optimized = optimized();
        KeepOrOptimize distinctActualOptimized = optimized.contains("DISTINCT")
                ? KeepOrOptimize.KEPT
                : KeepOrOptimize.OPTIMIZED;
        assertEquals(optimized, distinctExpectedOptimized, distinctActualOptimized);
        
    }

    public DistinctEliminatorSimpleTest(String sql, KeepOrOptimize distinctExpectedOptimized) {
        super(sql, sql, null, null);
        this.distinctExpectedOptimized = distinctExpectedOptimized;
    }
    
    public final KeepOrOptimize distinctExpectedOptimized;
    
    private enum KeepOrOptimize {
        KEPT, OPTIMIZED, IGNORED
    }
}
