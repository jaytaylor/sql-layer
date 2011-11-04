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

import java.io.IOException;
import java.io.Reader;
import java.io.FileReader;

import org.junit.Ignore;
import org.junit.Test;

/**
 * A base class for integration tests that use data from a YAML file to specify
 * the input and output expected from calls to the Postgres server.  Subclasses
 * are expected to be parameterized on the name of the YAML file.
 */
@Ignore
public class PostgresServerYamlITBase extends PostgresServerITBase {

    protected String filename;

    @Test
    public void testYaml() throws IOException {
	Reader in = new FileReader(filename);
	try {
	    new YamlTester(filename, in, connection).test();
	} finally {
	    in.close();
	}
    }

    /** Parameterized version. */
    protected PostgresServerYamlITBase(String filename) {
        this.filename = filename;
    }
}
