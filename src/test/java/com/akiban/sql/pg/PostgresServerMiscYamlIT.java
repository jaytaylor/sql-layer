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
    public void testYaml() throws Exception {
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
