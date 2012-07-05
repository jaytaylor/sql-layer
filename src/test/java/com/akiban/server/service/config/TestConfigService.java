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
        makeDataDirectory();
        ret.put(DATA_PATH_KEY, new Property(DATA_PATH_KEY, dataDirectory.getAbsolutePath()));
        final int bufferSize = Integer.parseInt(ret.get(BUFFER_SIZE_KEY).getValue());
        String memoryKey = BUFFER_MEMORY_KEY_PREFIX + "." + bufferSize;
        ret.put(memoryKey, new Property(memoryKey, UNIT_TEST_PERSISTIT_MEMORY));
        if (extraProperties != null) {
            for (final Property property : extraProperties) {
                ret.put(property.getKey(), property);
            }
        }
        ret.put(JOURNAL_SIZE_KEY, new Property(JOURNAL_SIZE_KEY, Long.toString(UNIT_TEST_PERSISTIT_JOURNAL_SIZE)));
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

    public static void setOverrides(Collection<Property> startupConfigProperties) {
        if (!startupConfigPropertiesRef.compareAndSet(null, startupConfigProperties)) {
            throw new IllegalStateException("already set"); // sanity check; feel free to remove if it gets in your way
        }
    }

    private static Collection<Property> getAndClearOverrides() {
        return startupConfigPropertiesRef.getAndSet(null);
    }

    public static boolean getDoCleanOnUnload() {
        return doCleanOnUnload;
    }

    public static void setDoCleanOnUnload(boolean doClean) {
        doCleanOnUnload = doClean;
    }

    private static final AtomicReference<Collection<Property>> startupConfigPropertiesRef = new AtomicReference<Collection<Property>>();
    public final static String DATA_PATH_KEY = "akserver.datapath";
    private final static String BUFFER_SIZE_KEY = "persistit.buffersize";
    private final static String BUFFER_MEMORY_KEY_PREFIX = "persistit.buffer.memory";
    private final static String JOURNAL_SIZE_KEY = "persistit.journalsize";
    private final static String UNIT_TEST_PERSISTIT_MEMORY = "20M";
    private final static long UNIT_TEST_PERSISTIT_JOURNAL_SIZE = 128 * 1024 * 1024;
}
