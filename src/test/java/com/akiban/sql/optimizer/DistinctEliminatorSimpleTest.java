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
package com.akiban.sql.optimizer;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.StatementNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class DistinctEliminatorSimpleTest extends OptimizerTestBase {

    public static final File RESOURCE_DIR = 
        new File(OptimizerTestBase.RESOURCE_DIR, "eliminate-distincts");
    private static final File SIMPLE_TEST = new File(RESOURCE_DIR, "simple-distincts.txt");

    @Before
    public void loadDDL() throws Exception {
        loadSchema(new File(RESOURCE_DIR, "schema.ddl"));
        ((BoundNodeToString)unparser).setUseBindings(true);
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> statements() throws Exception {
        FileReader fileReader = new FileReader(SIMPLE_TEST);
        try {
            ParameterizationBuilder pb = new ParameterizationBuilder();
            BufferedReader buffered = new BufferedReader(fileReader);
            int lineNo = 0;
            Set<String> pbNames = new HashSet<String>();
            for (String line; (line = buffered.readLine()) != null; ) {
                ++lineNo;
                line = line.trim();
                if (line.length() == 0)
                    continue;
                if (line.startsWith("#")) {
                    String name = name(lineNo, line.substring(1));
                    pb.addFailing(name, null, null);
                    continue;
                }
                String[] split = line.split("\\s+", 2);
                if (split.length != 2)
                    throw new RuntimeException(lineNo + ": " + Arrays.toString(split));

                String keepOrOptimize = split[0];
                String sql = split[1];
                String name = sql;
                name = name(lineNo, name);

                KeepOrOptimize distinctIsOptimized;
                if ("keep".equalsIgnoreCase(keepOrOptimize))
                    distinctIsOptimized = KeepOrOptimize.KEPT;
                else if ("optimize".equalsIgnoreCase(keepOrOptimize))
                    distinctIsOptimized = KeepOrOptimize.OPTIMIZED;
                else
                    throw new RuntimeException(lineNo + ": first word must be 'keep' or 'optimize'");

                if (!pbNames.add(name))
                    throw new RuntimeException("duplicate at line " + lineNo + ": " + name);

                pb.add(name, sql, distinctIsOptimized);
            }
            return pb.asList();
        }
        finally {
            fileReader.close();
        }
    }

    private static String name(int lineNo, String name) {
        if (name.startsWith("SELECT DISTINCT"))
            name = name.substring("SELECT DISTINCT".length());
        name = lineNo + ": " + name;
        return name;
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
    
    private String optimized() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        binder.bind(stmt);
        stmt = booleanNormalizer.normalize(stmt);
        typeComputer.compute(stmt);
        stmt = subqueryFlattener.flatten((DMLStatementNode)stmt);
        stmt = distinctEliminator.eliminate((DMLStatementNode)stmt);
        return unparser.toString(stmt);
    }

    public DistinctEliminatorSimpleTest(String sql, KeepOrOptimize distinctExpectedOptimized) {
        super(sql, sql, null, null);
        this.distinctExpectedOptimized = distinctExpectedOptimized;
    }
    
    public final KeepOrOptimize distinctExpectedOptimized;
    
    private enum KeepOrOptimize {
        KEPT, OPTIMIZED
    }
}
