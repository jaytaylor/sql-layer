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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class TestConfigService extends ConfigurationServiceImpl {
    private final static File TESTDIR = new File("/tmp/akserver-junit");
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
    protected Map<Property.Key, Property> loadProperties()
            throws IOException {
        Map<Property.Key, Property> ret = new HashMap<Property.Key, Property>(
                super.loadProperties());
        tmpDir = makeTempDatapathDirectory();
        Property.Key datapathKey = Property.parseKey("akserver.datapath");
        ret.put(datapathKey,
                new Property(datapathKey, tmpDir.getAbsolutePath()));
        Property.Key fixedKey = Property.parseKey("akserver.fixed");
        ret.put(fixedKey, new Property(fixedKey, "true"));
        if (extraProperties != null) {
            for (final Property property : extraProperties) {
                ret.put(property.getKey(), property);
            }
        }
        return ret;
    }

    @Override
    protected void unloadProperties() throws IOException {
        AkServerUtil.cleanUpDirectory(tmpDir);
    }

    @Override
    protected Set<Property.Key> getRequiredKeys() {
        return Collections.emptySet();
    }

    private File makeTempDatapathDirectory() throws IOException {
        if (TESTDIR.exists()) {
            if (!TESTDIR.isDirectory()) {
                throw new IOException(TESTDIR
                        + " exists but isn't a directory");
            }
        } else {
            if (!TESTDIR.mkdir()) {
                throw new IOException("Couldn't create dir: " + TESTDIR);
            }
            TESTDIR.deleteOnExit();
        }

        File tmpFile = File.createTempFile("akserver-unitdata", "", TESTDIR);
        if (!tmpFile.delete()) {
            throw new IOException("Couldn't delete file: " + tmpFile);
        }
        if (!tmpFile.mkdir()) {
            throw new IOException("Couldn't create dir: " + tmpFile);
        }
        tmpFile.deleteOnExit();
        return tmpFile;
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
}
