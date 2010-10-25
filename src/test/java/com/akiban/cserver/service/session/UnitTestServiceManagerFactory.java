package com.akiban.cserver.service.session;

import com.akiban.cserver.service.DefaultServiceManagerFactory;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.config.Property;
import com.akiban.cserver.store.PersistitStore;

import java.io.File;
import java.io.IOException;
import java.util.*;

public final class UnitTestServiceManagerFactory extends DefaultServiceManagerFactory {

    private static class TestConfigService extends ConfigurationServiceImpl {
        @Override
        protected Map<Property.Key, Property> loadProperties() throws IOException {
            Map<Property.Key, Property> ret = new HashMap<Property.Key, Property>(super.loadProperties());

            Property.Key datapathKey = new Property.Key("cserver", "datapath");
            ret.put(datapathKey, new Property(datapathKey, "/tmp/data"));

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
    public static PersistitStore getStoreForUnitTests() throws Exception {
        final ConfigurationServiceImpl stubConfig = new TestConfigService();
        stubConfig.start();
        final PersistitStore store = new PersistitStore(stubConfig) {
//            @Override
//            public void start() throws Exception {
//                File datadir = new File(stubConfig.getProperty("cserver", "datapath"));
//                int contents = datadir.list().length;
//                if (contents != 0) {
//                    throw new Exception(String.format("%s is not empty: %s", datadir, Arrays.asList(datadir.list())));
//                }
//                super.start();
//            }

            @Override
            public void stop() throws Exception {
                super.stop();
                File datadir = new File(stubConfig.getProperty("cserver", "datapath"));
                stubConfig.stop();
                for (File datafile : datadir.listFiles()) {
                    if (!datafile.delete()) {
                        throw new Exception("Failed to delete file: " + datafile);
                    }
                }

            }
        };
        store.start();
        return store;
    }
}
