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

package com.foundationdb.server.service.config;

import com.foundationdb.server.error.ConfigurationPropertiesLoadException;

import com.foundationdb.server.service.text.FullTextIndexServiceImpl;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import com.google.inject.Inject;

public class TestConfigService extends ConfigurationServiceImpl {
    private final static File TESTDIR = new File("/tmp/fdb-sql-layer");
    private final static String TEST_FDB_ROOT_DIR = "sql_test";
    private static File dataDirectory = null;
    private static int dataDirectoryCounter = 0;
    private static volatile boolean doCleanOnUnload = false;
    private final Map<String, String> extraProperties;

    @Inject
    public TestConfigService() {
        this.extraProperties = getAndClearOverrides();
    }

    @Override
    public boolean testing()
    {
        return true;
    }

    @Override
    protected Map<String, String> loadProperties() {
        Map<String, String> ret = new HashMap<>(super.loadProperties());
        makeDataDirectory();
        ret.put(FullTextIndexServiceImpl.BACKGROUND_INTERVAL_PROPERTY, "1000");
        ret.put(DATA_PATH_KEY, dataDirectory.getAbsolutePath());
        ret.put(FDB_ROOT_DIR_KEY, TEST_FDB_ROOT_DIR);
        ret.put(TEXT_INDEX_PATH_KEY, dataDirectory.getAbsolutePath());
        final int bufferSize = Integer.parseInt(ret.get(BUFFER_SIZE_KEY));
        String memoryKey = BUFFER_MEMORY_KEY_PREFIX + "." + bufferSize;
        ret.put(memoryKey, UNIT_TEST_PERSISTIT_MEMORY);
        ret.put(COMMIT_POLICY_KEY, UNIT_TEST_COMMIT_POLICY);
        ret.put(JOURNAL_SIZE_KEY, Long.toString(UNIT_TEST_PERSISTIT_JOURNAL_SIZE));
        ret.put(BUCKET_COUNT_KEY, BUCKET_COUNT);
        ret.put(FEATURE_DDL_WITH_DML_KEY, "true");
        ret.put(FEATURE_SPATIAL_INDEX_KEY, "true");
        ret.put(FEATURE_DIRECT_ROUTINES_KEY, "true");
        // extra = test overrides
        if (extraProperties != null) {
            for (final Map.Entry<String, String> property : extraProperties.entrySet()) {
                ret.put(property.getKey(), property.getValue());
            }
        }
        return ret;
    }


    @Override
    protected Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    public static File dataDirectory() {
        if (dataDirectory == null)
            makeDataDirectory();
        return dataDirectory;
    }

    public static File newDataDirectory() {
        dataDirectoryCounter++;
        makeDataDirectory();
        return dataDirectory;
    }

    private static void makeDataDirectory() {
        String name = "data";
        if (dataDirectoryCounter > 0)
            name += dataDirectoryCounter;
        dataDirectory = new File(TESTDIR, name);
        if (dataDirectory.exists()) {
            if (!dataDirectory.isDirectory()) {
                throw new ConfigurationPropertiesLoadException(dataDirectory.getName(), " it exists but isn't a directory");
            }
        } else {
            if (!dataDirectory.mkdirs()) {
                throw new ConfigurationPropertiesLoadException(dataDirectory.getName(), " it couldn't be created");
            }
            dataDirectory.deleteOnExit();
        }
    }

    public static void setOverrides(Map<String, String> startupConfigProperties) {
        if (!startupConfigPropertiesRef.compareAndSet(null, startupConfigProperties)) {
            throw new IllegalStateException("already set"); // sanity check; feel free to remove if it gets in your way
        }
    }

    private static Map<String, String> getAndClearOverrides() {
        return startupConfigPropertiesRef.getAndSet(null);
    }

    public static boolean getDoCleanOnUnload() {
        return doCleanOnUnload;
    }

    public static void setDoCleanOnUnload(boolean doClean) {
        doCleanOnUnload = doClean;
    }

    private static final AtomicReference<Map<String, String>> startupConfigPropertiesRef
            = new AtomicReference<>();
    public final static String DATA_PATH_KEY = "persistit.datapath";
    public final static String FDB_ROOT_DIR_KEY = "fdbsql.fdb.root_directory";
    public final static String TEXT_INDEX_PATH_KEY = "fdbsql.text.indexpath";
    private final static String COMMIT_POLICY_KEY = "persistit.txnpolicy";
    private final static String BUFFER_SIZE_KEY = "persistit.buffersize";
    private final static String BUFFER_MEMORY_KEY_PREFIX = "persistit.buffer.memory";
    private final static String JOURNAL_SIZE_KEY = "persistit.journalsize";
    private final static String UNIT_TEST_PERSISTIT_MEMORY = "20M";
    private final static long UNIT_TEST_PERSISTIT_JOURNAL_SIZE = 128 * 1024 * 1024;
    private final static String UNIT_TEST_COMMIT_POLICY = "SOFT";
    public final static String BUCKET_COUNT_KEY = "fdbsql.index_statistics.bucket_count";
    private final static String BUCKET_COUNT = "32";

    public final static String FEATURE_DDL_WITH_DML_KEY = "fdbsql.feature.ddl_with_dml_on";
    public final static String FEATURE_SPATIAL_INDEX_KEY = "fdbsql.feature.spatial_index_on";
    public final static String FEATURE_DIRECT_ROUTINES_KEY = "fdbsql.feature.direct_routines_on";
}
