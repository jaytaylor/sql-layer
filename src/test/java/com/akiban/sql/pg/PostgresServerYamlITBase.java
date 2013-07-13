/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.sql.pg;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * A base class for integration tests that use data from YAML files to specify the input and output expected from calls
 * to the Postgres server.  Subclasses should call {@link #testYaml} with the file to use for the test.
 */
public class PostgresServerYamlITBase extends PostgresServerITBase
{
    /** Whether to enable debugging output. */
    protected static final boolean DEBUG = Boolean.getBoolean("test.DEBUG");

    protected PostgresServerYamlITBase() {
    }

    /** Run a test with YAML input from the specified file. */
    protected void testYaml(File file) throws Exception {
        if(DEBUG) {
            System.err.println("\nFile: " + file);
        }
        Throwable thrown = null;
        try(Reader in = new InputStreamReader(new FileInputStream(file), "UTF-8")) {
            new YamlTester(file.toString(), in, getConnection()).test();
            if(DEBUG) {
                System.err.println("Test passed");
            }
        } catch(Exception | AssertionError e) {
            thrown = e;
            throw e;
        } finally {
            if(thrown != null) {
                System.err.println("Test failed: " + thrown);
                try {
                    forgetConnection();
                } catch(Exception e2) {
                    // Ignore
                }
            }
        }
    }
}
