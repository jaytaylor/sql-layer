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
