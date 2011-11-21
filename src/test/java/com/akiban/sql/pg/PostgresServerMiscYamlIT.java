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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Run tests specified as YAML files that end with the .yaml extension.  By
 * default, searches for files recursively in the yaml resource directory,
 * running tests for files that start with 'test-'.
 */
@RunWith(NamedParameterizedRunner.class)
public class PostgresServerMiscYamlIT extends PostgresServerYamlITBase {

    private static final String CLASSNAME =
	PostgresServerMiscYamlIT.class.getName();

    /**
     * A regular expression matching the names of the YAML files in the
     * resource directory, not including the extension, to use for tests.
     */
    private static final String CASE_NAME_REGEXP =
	System.getProperty(CLASSNAME + ".CASE_NAME_REGEXP", "test-.*");

    /** The directory containing the YAML files. */
    private static final File RESOURCE_DIR;
    static {
	String s = System.getProperty(CLASSNAME + ".RESOURCE_DIR");
	RESOURCE_DIR = (s != null)
	    ? new File(s) : new File(PostgresServerITBase.RESOURCE_DIR, "yaml");
    }

    /** Whether to search the resource directory recursively for test files. */
    private static final boolean RECURSIVE =
	Boolean.valueOf(System.getProperty(CLASSNAME + ".RECURSIVE", "true"));

    private final File file;

    public PostgresServerMiscYamlIT(String caseName, File file) {
	this.file = file;
    }

    @Test
    public void testYaml() throws IOException {
	testYaml(file);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
	Collection<Object[]> params = new ArrayList<Object[]>();
	collectParams(RESOURCE_DIR,
		      Pattern.compile(CASE_NAME_REGEXP + "[.]yaml"), params);
	return NamedParamsTestBase.namedCases(params);
    }

    /**
     * Add files from the directory that match the pattern to params, recursing
     * if appropriate.
     */
    private static void collectParams(
	File directory, final Pattern pattern, final Collection<Object[]> params)
    {
	File[] files = directory.listFiles(new FileFilter() {
	    public boolean accept(File file) {
		if (RECURSIVE && file.isDirectory()) {
		    collectParams(file, pattern, params);
		} else {
		    String name = file.getName();
		    if (pattern.matcher(name).matches()) {
			params.add(
			    new Object[] {
				name.substring(0, name.length() - 5), file });
		    }
		}
		return false;
	    }
	});
	if (files == null) {
	    throw new RuntimeException(
		"Problem accessing directory: " + directory);
	}
    }
}
