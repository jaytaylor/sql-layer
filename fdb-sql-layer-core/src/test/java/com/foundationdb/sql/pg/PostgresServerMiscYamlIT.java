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
import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesServiceImpl;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.FullTextIndexServiceImpl;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.EmbeddedJDBCServiceImpl;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.joda.time.DateTimeZone;
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
    private static final String CLASSNAME = PostgresServerMiscYamlIT.class.getName();

    /**
     * A regular expression matching the names of the YAML files in the
     * resource directory, not including the extension, to use for tests.
     */
    private static final String CASE_NAME_REGEXP = System.getProperty(CLASSNAME + ".CASE_NAME_REGEXP", "test-routine-basic.*");

    /** The directory containing the YAML files. */
    private static final File RESOURCE_DIR;

    static {
        String s = System.getProperty(CLASSNAME + ".RESOURCE_DIR");
        RESOURCE_DIR = (s != null) ? new File(s) : new File(PostgresServerITBase.RESOURCE_DIR, "yaml");
    }

    /** Whether to search the resource directory recursively for test files. */
    private static final boolean RECURSIVE = Boolean.valueOf(System.getProperty(CLASSNAME + ".RECURSIVE", "true"));

    private final File file;

    public PostgresServerMiscYamlIT(String caseName, File file) {
        this.file = file;
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class)
                .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class)
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void testYaml() throws Exception {
        testYaml(file);
    }

    @Parameters(name="{0}")
    public static Iterable<Object[]> queries() throws Exception {
        Collection<Object[]> params = new ArrayList<>();
        collectParams(RESOURCE_DIR, Pattern.compile(CASE_NAME_REGEXP + "[.]yaml"), params);
        return params;
    }

    static {
        String timezone = "UTC";
        DateTimeZone.setDefault(DateTimeZone.forID(timezone));
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));
    }

    /**
     * Add files from the directory that match the pattern to params, recursing
     * if appropriate.
     */
    private static void collectParams(File directory, final Pattern pattern, final Collection<Object[]> params) {
        File[] files = directory.listFiles(
            new FileFilter()
            {
                public boolean accept(File file) {
                    if(RECURSIVE && file.isDirectory()) {
                        collectParams(file, pattern, params);
                    } else {
                        String name = file.getName();
                        if(pattern.matcher(name).matches()) {
                            params.add(new Object[]{ name.substring(0, name.length() - 5), file });
                        }
                    }
                    return false;
                }
            }
        );
        if(files == null) {
            throw new RuntimeException("Problem accessing directory: " + directory);
        }
    }
}
