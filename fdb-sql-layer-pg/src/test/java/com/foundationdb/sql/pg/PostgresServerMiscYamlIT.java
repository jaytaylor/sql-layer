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

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.FullTextIndexServiceImpl;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.test.YamlTestFinder;

import java.net.URL;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

/**
 * Run tests specified as YAML files that end with the .yaml extension.  By
 * default, searches for files recursively in the yaml resource directory,
 * running tests for files that start with 'test-'.
 */
@RunWith(SelectedParameterizedRunner.class)
public class PostgresServerMiscYamlIT extends PostgresServerYamlITBase
{
    private final URL url;

    public PostgresServerMiscYamlIT(String caseName, URL url) {
        this.url = url;
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        // Get embedded JDBC and substitute working full text.
        return super.serviceBindingsProvider()
                .require(EmbeddedJDBCService.class)
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void testYaml() throws Exception {
        testYaml(url);
    }

    @Parameters(name="{0}")
    public static Iterable<Object[]> queries() throws Exception {
        return YamlTestFinder.findTests();
    }

}
