package com.akiban.cserver.service;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.config.Property;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.jmx.JmxRegistryServiceImpl;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.network.NetworkServiceImpl;

/**
 * Extension of DefaultServiceManagerFactory that creates mock services for unit tests.
 * Specifically, this class is used by tests that need to run the CServer and
 * PersistitStore code methods, but which do not need the JmxRegistryService
 * and NetworkService implementations to be functional.
 * @author peter
 *
 */
public class UnitTestServiceManagerFactory extends DefaultServiceManagerFactory
{
    private final static File TESTDIR = new File("/tmp/cserver-junit");
    private final MockJmxRegistryService jmxRegistryService = new MockJmxRegistryService();
    private final TestConfigService configService = new TestConfigService();
    private final MockNetworkService networkService = new MockNetworkService(configService);
    
    private final boolean withNetwork;
    
    public static ServiceManagerImpl createServiceManager() {
        return new ServiceManagerImpl(new UnitTestServiceManagerFactory(false));
    }
    
    public static ServiceManagerImpl createServiceManagerWithNetworkService() {
        return new ServiceManagerImpl(new UnitTestServiceManagerFactory(true));
    }
    
    public static ServiceManager getServiceManager() {
        return ServiceManagerImpl.get();
    }
    
    private static class MockJmxRegistryService extends JmxRegistryServiceImpl {
        @Override
        public void register(JmxManageable service) {
            // ignore
        }
        @Override
        public void unregister(String serviceName) {
            // ignore
        }
        
        @Override
        public void start() {
            // ignore
        }
        
        @Override
        public void stop() {
            // ignore
        }
    }
    
    private static class MockNetworkService implements NetworkService, Service<NetworkService> {

        public MockNetworkService(ConfigurationService config) {
            
        }

        @Override
        public String getNetworkHost() {
          throw new UnsupportedOperationException();
        }

        @Override
        public int getNetworkPort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getTcpNoDelay() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getNumberOpenConnections() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTotalNumberConnections() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NetworkService cast() {
            return this;
        }

        @Override
        public Class<NetworkService> castClass() {
            return NetworkService.class;
        }

        @Override
        public void start() throws Exception {
           // do nothing
        }

        @Override
        public void stop() throws Exception {
           // do nothing 
        }
        

    }
    
    
    private static class TestConfigService extends ConfigurationServiceImpl {
        File tmpDir;
        
        @Override
        protected boolean shouldLoadAdminProperties() {
            return false;
        }

        @Override
        protected Map<Property.Key, Property> loadProperties() throws IOException {
            Map<Property.Key, Property> ret = new HashMap<Property.Key, Property>(super.loadProperties());
            tmpDir = makeTempDatapathDirectory();
            Property.Key datapathKey = new Property.Key("cserver", "datapath");
            ret.put(datapathKey, new Property(datapathKey, tmpDir.getAbsolutePath()));
            Property.Key fixedKey = new Property.Key("cserver", "fixed");
            ret.put(fixedKey, new Property(fixedKey, "true"));
            return ret;
        }
        
        @Override
        protected void unloadProperties() throws IOException {
            CServerUtil.cleanUpDirectory(tmpDir);
        }

        @Override
        protected Set<Property.Key> getRequiredKeys() {
            return Collections.emptySet();
        }
        
        private File makeTempDatapathDirectory() throws IOException {
            if (TESTDIR.exists()) {
                if (!TESTDIR.isDirectory()) {
                    throw new IOException(TESTDIR + " exists but isn't a directory");
                }
            }
            else {
                if (!TESTDIR.mkdir()) {
                    throw new IOException("Couldn't create dir: " + TESTDIR);
                }
                TESTDIR.deleteOnExit();
            }

            File tmpFile = File.createTempFile("cserver-unitdata", "", TESTDIR);
            if (!tmpFile.delete()) {
                throw new IOException("Couldn't delete file: " + tmpFile);
            }
            if (!tmpFile.mkdir()) {
                throw new IOException("Couldn't create dir: " + tmpFile);
            }
            tmpFile.deleteOnExit();
            return tmpFile;
        }
    }
    
    private UnitTestServiceManagerFactory(final boolean withNetwork) {
        this.withNetwork = withNetwork;
    }

    @Override
    public Service<JmxRegistryService> jmxRegistryService() {
        return jmxRegistryService;
    }
    
    @Override
    public Service<ConfigurationService> configurationService() {
        return configService;
    }
    
    @Override
    public Service<NetworkService> networkService() {
        if (withNetwork) {
            return new NetworkServiceImpl(configService);
        }
        return networkService;
    }
}
