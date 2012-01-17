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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class DistinctEliminatorSimpleTest extends DistinctEliminatorTestBase {

    private static final File SIMPLE_TEST = new File(RESOURCE_DIR, "simple-distincts.txt");

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
                if (line.length() == 0 || line.startsWith("#"))
                    continue;
                //Pattern.compile("(!)?\\s*(keep|optimize)\\s+((SELECT DISTINCT)?.*)?", Pattern.CASE_INSENSITIVE);
                Matcher matcher = LINE_PATTERN.matcher(line);
                if (!matcher.find())
                    throw new RuntimeException(lineNo + ": " + line);
                    
                String ignoredStr = matcher.group(1);
                boolean ignored = ignoredStr != null;
                String keepOrOptimize = matcher.group(2);
                String sql = matcher.group(3);
                String selectDistinct = matcher.group(4);

                String name = sql;
                if (name.startsWith("SELECT DISTINCT"))
                    name = name.substring("SELECT DISTINCT".length());
                name = lineNo + ": " + name;

                KeepOrOptimize distinctIsOptimized;
                if ("keep".equalsIgnoreCase(keepOrOptimize))
                    distinctIsOptimized = KeepOrOptimize.KEPT;
                else if ("optimize".equalsIgnoreCase(keepOrOptimize))
                    distinctIsOptimized = KeepOrOptimize.OPTIMIZED;
                else
                    throw new RuntimeException(lineNo + ": first word must be 'keep' or 'optimize'");
                
                if (selectDistinct == null)
                    throw new RuntimeException(lineNo + ": must be a SELECT DISTINCT");

                if (!pbNames.add(name))
                    throw new RuntimeException("duplicate at line " + lineNo + ": " + name);
                pb.create(name, !ignored, sql, distinctIsOptimized);
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
    
    private static final Pattern LINE_PATTERN
            = Pattern.compile("(!)?\\s*(keep|optimize)\\s+((SELECT\\s+DISTINCT)?.*)?", Pattern.CASE_INSENSITIVE);
    
    private enum KeepOrOptimize {
        KEPT, OPTIMIZED
    }
}
