package com.akiban.cserver.service.session;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.akiban.cserver.service.DefaultServiceManagerFactory;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.config.Property;
import com.akiban.cserver.service.persistit.PersistitService;
import com.akiban.cserver.service.persistit.PersistitServiceImpl;
import com.akiban.cserver.store.PersistitStore;

public class UnitTestServiceManagerFactory extends DefaultServiceManagerFactory
{
    private static class TestConfigService extends ConfigurationServiceImpl {
        @Override
        protected boolean shouldLoadAdminProperties() {
            return false;
        }

        @Override
        protected Map<Property.Key, Property> loadProperties() throws IOException {
            Map<Property.Key, Property> ret = new HashMap<Property.Key, Property>(super.loadProperties());

            File cserverUnitDir = new File("/tmp/cserver-junit");
            if (cserverUnitDir.exists()) {
                if (!cserverUnitDir.isDirectory()) {
                    throw new IOException(cserverUnitDir + " exists but isn't a directory");
                }
            }
            else {
                if (!cserverUnitDir.mkdir()) {
                    throw new IOException("Couldn't create dir: " + cserverUnitDir);
                }
                cserverUnitDir.deleteOnExit();
            }

            File tmpFile = File.createTempFile("cserver-unitdata", "", cserverUnitDir);
            if (!tmpFile.delete()) {
                throw new IOException("Couldn't delete file: " + tmpFile);
            }
            if (!tmpFile.mkdir()) {
                throw new IOException("Couldn't create dir: " + tmpFile);
            }
            tmpFile.deleteOnExit();
            Property.Key datapathKey = new Property.Key("cserver", "datapath");
            ret.put(datapathKey, new Property(datapathKey, tmpFile.getAbsolutePath()));

            Property.Key fixedKey = new Property.Key("cserver", "fixed");
            ret.put(fixedKey, new Property(fixedKey, "true"));

            return ret;
        }

        @Override
        protected Set<Property.Key> getRequiredKeys() {
            return Collections.emptySet();
        }
    }

    public static class UnitTestPersistitService extends PersistitServiceImpl
    {
        private final ConfigurationService stubConfig;

        private UnitTestPersistitService(ConfigurationService config) throws Exception {
            super(config);
            stubConfig = config;
        }

        @Override
        public synchronized void start() throws Exception {
            File datadir = new File(stubConfig.getProperty("cserver", "datapath"));
            int contents = datadir.list().length;
            if (contents != 0) {
                throw new Exception(String.format("%s is not empty: %s", datadir, Arrays.asList(datadir.list())));
            }
            super.start();
        }

        public synchronized void superStart() throws Exception {
            super.start();
        }

        public synchronized void superStop() throws Exception {
            super.stop();
        }

        @Override
        public synchronized void stop() throws Exception
        {
            super.stop();
            File datadir = new File(stubConfig.getProperty("cserver", "datapath"));
            Set<File> failedToDelete = new HashSet<File>();
            for (File dataFile : datadir.listFiles())
            {
                boolean fileDeleted = dataFile.delete();
                if (!fileDeleted)
                {
                    failedToDelete.add(dataFile);
                }
            }
            if (!failedToDelete.isEmpty())
            {
                StringBuffer error = new StringBuffer();
                error.append("Failed to delete the following files: \n");
                for (File file : failedToDelete)
                {
                    error.append(file.getAbsolutePath()).append("\n");
                }
                throw new Exception(error.toString());
            }
        }
    }

    ConfigurationServiceImpl configService = null;

    @Override
    public Service<ConfigurationService> configurationService() {
        if (configService == null) {
            configService = new TestConfigService();
        }
        return configService;
    }

    // TODO - this is a temporary way for unit tests to get a configured PersistitStore.
    public static PersistitStore getStoreForUnitTests() throws Exception
    {
        final ConfigurationServiceImpl stubConfig = new TestConfigService();
        stubConfig.start();
        final PersistitService persistitService = new UnitTestPersistitService(stubConfig);
        persistitService.start();
        PersistitStore store = new PersistitStore(persistitService) {
            // TODO - temporary hack to stop the PersistitService (which now has a
            // separate lifecycle) within unit tests.  Fix the tests.
            public void stop() throws Exception {
                super.stop();
                persistitService.stop();
            }
        };
        store.start();
        return store;
    }
}
