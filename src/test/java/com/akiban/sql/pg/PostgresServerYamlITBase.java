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

import com.akiban.server.error.InvalidOperationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * A base class for integration tests that use data from YAML files to specify
 * the input and output expected from calls to the Postgres server.  Subclasses
 * should call {@link #testYaml} with the file to use for the test.
 */
@Ignore
public class PostgresServerYamlITBase {

    /** Whether to enable debugging output. */
    protected static final boolean DEBUG = Boolean.getBoolean("test.DEBUG");

    private static final PostgresServerIT manageServer = new PostgresServerIT();

    protected PostgresServerYamlITBase() { }

    @BeforeClass
    public static void openTheConnection() throws Exception {
	manageServer.startTestServices();
        manageServer.ensureConnection();
    }

    @AfterClass
    public static void closeTheConnection() throws Exception {
	manageServer.stopTestServices();
        manageServer.closeTheConnection();
    }

    @Before
    public void dropAllTables() {
	manageServer.accessDropAllTables();
    }

    protected static Connection getConnection() throws Exception {
        return manageServer.ensureConnection();
    }

    protected static void forgetConnection() {
	if (DEBUG) {
	    System.err.println("Closing possibly damaged connection");
	}
        try {
            manageServer.closeTheConnection();
        }
        catch (Exception ex) {
        }
    }

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
	Connection connection = getConnection();
        Throwable exception = null;
	Reader in = null;
	try {
	    in = new InputStreamReader(new FileInputStream(file), "UTF-8");
	    new YamlTester(file.toString(), in, connection).test();
	    if (DEBUG) {
		System.err.println("Test passed");
	    }
	} catch (Exception e) {
	    exception = e;
	    throw e;
	} catch (Error e) {
	    exception = e;
	    throw e;
	} finally {
	    if (exception != null) {
                System.err.println("Test failed: " + exception);
		forgetConnection();
	    }
	    if (in != null) {
		in.close();
	    }
	}
    }

    /**
     * Subclass of PostgresServerITBase to permit accessing its non-public
     * methods.
     */
    @Ignore
    private static class PostgresServerIT extends PostgresServerITBase {
	void accessDropAllTables() throws InvalidOperationException {
	    dropAllTables();
	}
	Connection ensureConnection() throws Exception {
	    if (connection == null)
                openTheConnection();
            return connection;
	}
    }
}
