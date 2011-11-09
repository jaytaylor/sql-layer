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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.RegexFilenameFilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Run tests specified as YAML files in the yaml-misc resource directory that
 * end with the .yaml extension.  By default, runs tests for files that start
 * with 'test-'.
 */
@RunWith(NamedParameterizedRunner.class)
public class PostgresServerMiscYamlIT extends PostgresServerYamlITBase {

    /**
     * A regular expression matching the names of the YAML files in the
     * resource directory, not including the extension, to use for tests.
     */
    private static final String FILENAME_REGEXP =
	System.getProperty(PostgresServerMiscYamlIT.class.getName() +
			   ".FILENAME_REGEXP", "test-.*");

    private static final File RESOURCE_DIR =
        new File(PostgresServerITBase.RESOURCE_DIR, "yaml-misc");

    private final File file;

    public PostgresServerMiscYamlIT(String filename) {
	file = new File(filename);
    }

    @Test
    public void testYaml() throws IOException {
	testYaml(file);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
	Collection<Object[]> params = new ArrayList<Object[]>();
	File[] files = RESOURCE_DIR.listFiles(
	    new RegexFilenameFilter(FILENAME_REGEXP + "[.]yaml"));
	for (File file : files) {
	    params.add(new Object[] { file.toString() });
	}
	return NamedParamsTestBase.namedCases(params);
    }
}
