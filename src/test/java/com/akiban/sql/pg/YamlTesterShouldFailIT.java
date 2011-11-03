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

package com.akiban.sql.pg;

import org.junit.Test;
import static org.junit.Assert.fail;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.RegexFilenameFilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.runner.RunWith;

/**
 * Run basic tests of {@code YamlTester} for YAML files that specify failing
 * tests.
 */
@RunWith(NamedParameterizedRunner.class)
public class YamlTesterShouldFailIT extends PostgresServerYamlITBase {

    private static final File RESOURCE_DIR =
        new File(PostgresServerITBase.RESOURCE_DIR, "yaml-misc");

    public YamlTesterShouldFailIT(String caseName) {
        super(caseName);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
	Collection<Object[]> params = new ArrayList<Object[]>();
	File[] files = RESOURCE_DIR.listFiles(
	    new RegexFilenameFilter("test-fail.*[.]yaml"));
	for (File file : files) {
	    params.add(new Object[] { file.toString() });
	}
	return NamedParamsTestBase.namedCases(params);
    }

    @Test
    @Override
    public void testYaml() throws IOException {
	try {
	    super.testYaml();
	    fail("Expected exception");
	} catch (Throwable t) {
	    System.err.println("testYaml: " + t);
	}
    }
}
