/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
    public boolean testing()
    {
        return true;
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
