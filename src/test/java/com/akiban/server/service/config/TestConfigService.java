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

package com.akiban.server.service.config;

import com.akiban.server.AkServerUtil;
import com.akiban.server.error.ConfigurationPropertiesLoadException;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class TestConfigService extends ConfigurationServiceImpl {
    public final static File TESTDIR = new File("/tmp/akserver-junit");
    private final Collection<Property> extraProperties;
    File tmpDir;

    public TestConfigService() {
        this.extraProperties = getAndClearOverrides();
    }

    @Override
    protected boolean shouldLoadAdminProperties() {
        return false;
    }

    @Override
    protected Map<String, Property> loadProperties() {
        Map<String, Property> ret = new HashMap<String, Property>(super.loadProperties());
        tmpDir = makeTempDatapathDirectory();
        String datapathKey = "akserver.datapath";
        ret.put(datapathKey, new Property(datapathKey, tmpDir.getAbsolutePath()));
        final int bufferSize = Integer.parseInt(ret.get("persistit.buffersize").getValue());
        String memoryKey = "persistit.buffer.memory." + bufferSize;
        ret.put(memoryKey, new Property(memoryKey, UNIT_TEST_PERSISTIT_MEMORY));
        if (extraProperties != null) {
            for (final Property property : extraProperties) {
                ret.put(property.getKey(), property);
            }
        }
        String journalSizeKey = "persistit.journalsize";

        ret.put(journalSizeKey, new Property(journalSizeKey, Integer.toString(128 * 1024 * 1024)));
        return ret;
    }

    @Override
    protected void unloadProperties() {
        AkServerUtil.cleanUpDirectory(tmpDir);
    }

    @Override
    protected Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    private File makeTempDatapathDirectory() {
        if (TESTDIR.exists()) {
            if (!TESTDIR.isDirectory()) {
                throw new ConfigurationPropertiesLoadException(TESTDIR.getName(), " it exists but isn't a directory");
            }
        } else {
            if (!TESTDIR.mkdir()) {
                throw new ConfigurationPropertiesLoadException (TESTDIR.getName(), " it couldn't be created");
            }
            TESTDIR.deleteOnExit();
        }
        return TESTDIR;
//
//        File tmpFile;
//        try {
//            tmpFile = File.createTempFile("akserver-unitdata", "", TESTDIR);
//        } catch (IOException e) {
//            throw new ConfigurationPropertiesLoadException ("akserver-unitdata", "it could create the temp file");
//        }
//        if (!tmpFile.delete()) {
//            throw new ConfigurationPropertiesLoadException (tmpFile.getName(), "it couldn't be deleted");
//        }
//        if (!tmpFile.mkdir()) {
//            throw new ConfigurationPropertiesLoadException (tmpFile.getName(), "it couldn't be created");
//        }
//        tmpFile.deleteOnExit();
//        return tmpFile;
    }

    public static void setOverrides(Collection<Property> startupConfigProperties) {
        if (!startupConfigPropertiesRef.compareAndSet(null, startupConfigProperties)) {
            throw new IllegalStateException("already set"); // sanity check; feel free to remove if it gets in your way
        }
    }

    private static Collection<Property> getAndClearOverrides() {
        return startupConfigPropertiesRef.getAndSet(null);
    }

    private static final AtomicReference<Collection<Property>> startupConfigPropertiesRef = new AtomicReference<Collection<Property>>();
    private final static String UNIT_TEST_PERSISTIT_MEMORY = "20M";
}
