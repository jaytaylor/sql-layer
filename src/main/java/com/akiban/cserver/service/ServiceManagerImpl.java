package com.akiban.cserver.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.jmx.JmxRegistryServiceImpl;
import com.akiban.cserver.service.network.NetworkServiceImpl;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.Store;

public class ServiceManagerImpl implements ServiceManager {
    private Map<String, Service> services;

    private static final String NETWORK = "network";
    private static final String CONFIGURATION = "configuration";
    private static final String JMX = "jmx";
    private static final String STORE = "store";
    private static final String CSERVER = "cserver";

    // PDB: temporarily this is public so that CServer.main and tests can
    // construct one
    public ServiceManagerImpl() {
        services = new LinkedHashMap<String, Service>();
    }

    @Override
    public CServer getCServer() {
        return (CServer) getService(CSERVER);
    }

    @Override
    public Store getStore() {
        return (Store) getService(STORE);
    }

    public void startServices() throws Exception {
        JmxRegistryServiceImpl jmxRegistry = new JmxRegistryServiceImpl();

        startAndPut(new ConfigurationServiceImpl(), CONFIGURATION);
        startAndPut(createPersistitStore(), STORE);
        startAndPut(new NetworkServiceImpl(), NETWORK);
        startAndPut(new CServer(this), CSERVER);
        startAndPut(jmxRegistry, JMX);

        for (Service service : services.values()) {
            if (service instanceof JmxManageable) {
                jmxRegistry.register((JmxManageable) service);
            }
        }
    }

    private void startAndPut(Service service, String name) throws Exception {
        service.start();
        services.put(name, service);
    }

    public void stopServices() throws Exception {
        List<Service> stopServices = new ArrayList<Service>(services.size());
        for (Service service : services.values()) {
            stopServices.add(service);
        }
        ListIterator<Service> reverseIter = stopServices
                .listIterator(stopServices.size());
        while (reverseIter.hasPrevious()) {
            reverseIter.previous().stop();
        }
    }

    public Service getService(final String name) {
        return services.get(name);
    }

    /**
     * Quick-and-dirty testing tool. Creates a ServiceManager, starts its
     * services (including JMX), and then just sits around waiting to be killed.
     * 
     * @param ignored
     *            ignored
     */
    public static void main(String[] ignored) throws Exception {
        ServiceManagerImpl sm = new ServiceManagerImpl();
        sm.startServices();
        Object foo = new Object();
        synchronized (foo) {
            try {
                foo.wait();
            } catch (InterruptedException e) {
                System.err.println("Not sure how we got interrupted!");
                e.printStackTrace();
            }
        }
    }

    private CServerConfig config;

    public CServerConfig getConfig() {
        return config;
    }

    public void setupCServerConfig() throws Exception {
        config = new CServerConfig();
        config.load();
        if (config.getException() != null) {
            throw config.getException();
        }
    }

    public void setupCServerConfigForUnitTests() throws Exception {
        config = CServerConfig.unitTestConfig();
    }

    private PersistitStore createPersistitStore() throws Exception {
        if (config == null) {
            throw new IllegalStateException("No configuration");
        }
        PersistitStore store = new PersistitStore(config);
        return store;
    }

    // TODO - this is a temporary way for unit tests to get a configured
    // PersistitStore.
    public static PersistitStore getStoreForUnitTests() throws Exception {
        ServiceManagerImpl sm = new ServiceManagerImpl();
        sm.setupCServerConfigForUnitTests();
        final PersistitStore store = sm.createPersistitStore();
        store.start();
        return store;
    }
}
