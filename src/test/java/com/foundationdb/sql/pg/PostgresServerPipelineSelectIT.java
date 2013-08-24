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

import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RunWith(NamedParameterizedRunner.class)
public class PostgresServerPipelineSelectIT extends PostgresServerSelectIT 
{
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "pipeline-select");

    @Override
    protected Map<String, String> startupConfigProperties() {
        Properties loadedProperties = new Properties();
        try {
            FileInputStream istr = new FileInputStream(new File(RESOURCE_DIR,
                                                                "pipeline.properties"));
            loadedProperties.load(istr);
            istr.close();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        Map<String, String> properties = new HashMap<>();
        for (String key : loadedProperties.stringPropertyNames()) {
            properties.put(key, loadedProperties.getProperty(key));
        }
        return properties;
    }

    @Override
    public void loadDatabase() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        return NamedParamsTestBase.namedCases(TestBase.sqlAndExpectedAndParams(RESOURCE_DIR));
    }

    public PostgresServerPipelineSelectIT(String caseName, String sql, 
                                          String expected, String error,
                                          String[] params) {
        super(caseName, sql, expected, error, params);
    }

}
