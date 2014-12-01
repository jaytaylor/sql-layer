/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.pg;

import com.foundationdb.sql.test.YamlTester;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

/**
 * A base class for integration tests that use data from YAML files to specify the input and output expected from calls
 * to the Postgres server.  Subclasses should call {@link #testYaml} with the file to use for the test.
 */
public class PostgresServerYamlITBase extends PostgresServerITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(PostgresServerYamlITBase.class);

    protected PostgresServerYamlITBase() {
    }

    protected boolean isRandomCost(){
        return false;
    }

    /** Run a test with YAML input from the specified URL. */
    protected void testYaml(URL url) throws Exception {
        LOG.debug("URL: {}", url);
        Throwable thrown = null;
        try(Reader in = new InputStreamReader(url.openStream(), "UTF-8")) {
            new YamlTester(url, in, getConnection(), isRandomCost())
                .test();
            LOG.debug("Test passed");
        } catch(Exception | AssertionError e) {
            thrown = e;
            throw e;
        } finally {
            if(thrown != null) {
                LOG.error("Test failed", thrown);
                try {
                    forgetConnection();
                } catch(Exception e2) {
                    // Ignore
                }
            }
        }
    }

}
