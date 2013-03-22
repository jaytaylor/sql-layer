
package com.akiban.sql.pg;

import com.akiban.server.error.InvalidOperationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;

import com.akiban.server.test.it.ITBase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * A base class for integration tests that use data from YAML files to specify
 * the input and output expected from calls to the Postgres server.  Subclasses
 * should call {@link #testYaml} with the file to use for the test.
 */
@Ignore
public class PostgresServerYamlITBase extends PostgresServerITBase {

    /** Whether to enable debugging output. */
    protected static final boolean DEBUG = Boolean.getBoolean("test.DEBUG");

    protected PostgresServerYamlITBase() { }

    /**
     * Run a test with YAML input from the specified file.
     *
     * @param file the file
     * @throws IOException if there is an error accessing the file
     */
    protected void testYaml(File file) throws Exception {
	if (DEBUG) {
	    System.err.println("\nFile: " + file);
	}
	Exception exception = null;
	Reader in = null;
	try {
	    in = new InputStreamReader(new FileInputStream(file), "UTF-8");
	    new YamlTester(file.toString(), in, getConnection()).test();
	    if (DEBUG) {
		System.err.println("Test passed");
	    }
	} catch (Exception e) {
	    exception = e;
	    throw e;
	} finally {
	    if (exception != null) {
		System.err.println("Test failed: " + exception);
		try {
                    forgetConnection();
                }
                catch (Exception e2) {
                }
	    }
	    if (in != null) {
		in.close();
	    }
	}
    }

}
