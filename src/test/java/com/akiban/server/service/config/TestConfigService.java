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

package com.akiban.server.service.config;

import com.akiban.server.AkServerUtil;
import com.akiban.server.error.ConfigurationPropertiesLoadException;
import com.akiban.server.service.plugins.Plugin;
import com.akiban.server.service.plugins.PluginsFinder;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class TestConfigService extends ConfigurationServiceImpl {
    private final static File TESTDIR = new File("/tmp/akserver-junit");
    private static File dataDirectory = null;
    private static int dataDirectoryCounter = 0;
    private static volatile boolean doCleanOnUnload = false;
    private final Map<String, String> extraProperties;
    File tmpDir;

    private static final PluginsFinder emptyPluginsFinder = new PluginsFinder() {
        @Override
        public Collection<? extends Plugin> get() {
            return Collections.emptyList();
        }
    };

    public TestConfigService() {
        super(emptyPluginsFinder);
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
        ret.put(DATA_PATH_KEY, dataDirectory.getAbsolutePath());
        ret.put(TEXT_INDEX_PATH_KEY, dataDirectory.getAbsolutePath());
        final int bufferSize = Integer.parseInt(ret.get(BUFFER_SIZE_KEY));
        String memoryKey = BUFFER_MEMORY_KEY_PREFIX + "." + bufferSize;
        ret.put(memoryKey, UNIT_TEST_PERSISTIT_MEMORY);
        ret.put(COMMIT_POLICY_KEY, UNIT_TEST_COMMIT_POLICY);
        if (extraProperties != null) {
            for (final Map.Entry<String, String> property : extraProperties.entrySet()) {
                ret.put(property.getKey(), property.getValue());
            }
        }
        ret.put(JOURNAL_SIZE_KEY, Long.toString(UNIT_TEST_PERSISTIT_JOURNAL_SIZE));
        ret.put(PARSE_SPATIAL_INDEX, "true");
        return ret;
    }

    @Override
    protected void unloadProperties() {
        if (doCleanOnUnload) {
            AkServerUtil.cleanUpDirectory(tmpDir);
        }
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
    public final static String DATA_PATH_KEY = "akserver.datapath";
    public final static String TEXT_INDEX_PATH_KEY = "akserver.text.indexpath";
    private final static String COMMIT_POLICY_KEY = "persistit.txnpolicy";
    private final static String BUFFER_SIZE_KEY = "persistit.buffersize";
    private final static String BUFFER_MEMORY_KEY_PREFIX = "persistit.buffer.memory";
    private final static String JOURNAL_SIZE_KEY = "persistit.journalsize";
    private final static String PARSE_SPATIAL_INDEX = "akserver.postgres.parserGeospatialIndexes";
    private final static String UNIT_TEST_PERSISTIT_MEMORY = "20M";
    private final static long UNIT_TEST_PERSISTIT_JOURNAL_SIZE = 128 * 1024 * 1024;
    private final static String UNIT_TEST_COMMIT_POLICY = "SOFT";
}
