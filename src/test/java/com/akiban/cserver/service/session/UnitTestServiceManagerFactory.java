package com.akiban.cserver.service.session;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.akiban.cserver.service.DefaultServiceManagerFactory;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.config.Property;
import com.akiban.cserver.store.PersistitStore;

public final class UnitTestServiceManagerFactory extends DefaultServiceManagerFactory
{
    private static class TestConfigService extends ConfigurationServiceImpl {
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

    ConfigurationServiceImpl configService = null;

    @Override
    public Service configurationService() {
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
        final PersistitStore store = new PersistitStore(stubConfig)
        {
            @Override
            public void start() throws Exception {
                File datadir = new File(stubConfig.getProperty("cserver", "datapath"));
                int contents = datadir.list().length;
                if (contents != 0) {
                    throw new Exception(String.format("%s is not empty: %s", datadir, Arrays.asList(datadir.list())));
                }
                super.start();
            }


            @Override
            public synchronized void stop() throws Exception
            {
                super.stop();

                File datadir = new File(stubConfig.getProperty("cserver", "datapath"));
                stubConfig.stop();
                Set<File> failedToDelete = new HashSet<File>();
                for (File dataFile : datadir.listFiles())
                {
                    boolean fileDeleted = dataFile.delete();
                    if (!fileDeleted)
                    {
                        failedToDelete.add(dataFile);
                    }
                }
                if (failedToDelete.isEmpty() == false)
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
        };
        store.start();
        return store;
    }
}
