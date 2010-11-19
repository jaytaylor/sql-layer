package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.schema.SchemaServiceImpl;
import com.akiban.cserver.service.session.SessionService;
import com.akiban.cserver.service.session.SessionServiceImpl;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.jmx.JmxRegistryServiceImpl;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.Store;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class ServiceManagerImpl implements ServiceManager, JmxManageable
{
    private static final AtomicReference<ServiceManager> instance = new AtomicReference<ServiceManager>(null);
    // TODO: Supply the factory externally.
    private final ServiceManagerFactory factory;
    private Map<String, Service> services;

    private final CountDownLatch blockerLatch = new CountDownLatch(1);

    private static final String NETWORK = "network";
    private static final String CONFIGURATION = "configuration";
    private static final String JMX = "jmx";
    private static final String STORE = "store";
    private static final String CSERVER = "cserver";
    private static final String SCHEMA = "schema";
    private static final String SESSION = "session";

    public static void setServiceManager(ServiceManager newInstance)
    {
        if (newInstance == null) {
            instance.set(null);
        }
        else if (!instance.compareAndSet(null, newInstance)) {
            throw new RuntimeException("Tried to install a ServiceManager, but one was already set");
        }
    }

    private ServiceManagerImpl()
    {
        this(new DefaultServiceManagerFactory());
    }
    /**
     * This constructor is made protected for unit testing.
     */
    protected ServiceManagerImpl(ServiceManagerFactory factory)
    {
        this.factory = factory;
        services = new LinkedHashMap<String, Service>();
    }

    public static ServiceManager get()
    {
        return instance.get();
    }

    @Override
    public CServer getCServer() {
        return (CServer) getService(CSERVER);
    }

    @Override
    public Store getStore() {
        return (Store) getService(STORE);
    }

    @Override
    public SessionService getSessionService() {
        return (SessionService) getService(SESSION);
    }

    public void startServices() throws Exception {

        JmxRegistryServiceImpl jmxRegistry = new JmxRegistryServiceImpl();
        startAndPut(jmxRegistry, JMX, jmxRegistry);
        startAndPut(factory.configurationService(), CONFIGURATION, jmxRegistry);

        jmxRegistry.register(this);
        ConfigurationService configService = (ConfigurationService) getService(CONFIGURATION);
        servicesDebugHooks(configService, jmxRegistry);

        // TODO: CServerConfig setup is still a mess. Clean up and move to DefaultServiceManagerFactory.
        startAndPut(new SessionServiceImpl(), SESSION, jmxRegistry);
        Store store = new PersistitStore(configService);
        startAndPut(store, STORE, jmxRegistry);
        startAndPut(factory.networkService(), NETWORK, jmxRegistry);
        startAndPut(factory.chunkserverService(), CSERVER, jmxRegistry);
        startAndPut(new SchemaServiceImpl( store.getSchemaManager() ), SCHEMA, jmxRegistry);
        setServiceManager(this);
    }

    private void servicesDebugHooks(ConfigurationService configService, JmxRegistryServiceImpl jmxRegistry)
    throws InterruptedException
    {
        if (configService.getProperty("services", "start_blocked", "false").equalsIgnoreCase("true")) {
            System.out.println("BLOCKING BLOCKING BLOCKING BLOCKING BLOCKING");
            System.out.println("  CServer is waiting for persmission to");
            System.out.println("  proceed from JMX.");
            System.out.println("BLOCKING BLOCKING BLOCKING BLOCKING BLOCKING");
            blockerLatch.await();
        }
        else {
            blockerLatch.countDown();
        }
    }

    private void startAndPut(Service service, String name, JmxRegistryService jmxRegistry) throws Exception {
        //System.out.println("Starting service: " + service.getClass()); // TODO change to logging
        service.start();
        services.put(name, service);
        if (service instanceof JmxManageable) {
            jmxRegistry.register((JmxManageable) service);
        }
    }

    public void stopServices() throws Exception {
        setServiceManager(null);
        List<Service> stopServices = new ArrayList<Service>(services.size());
        for (Service service : services.values()) {
            stopServices.add(service);
        }
        //System.out.println("Preparing to shut down services: " + stopServices); // TODO change to logging
        ListIterator<Service> reverseIter = stopServices
                .listIterator(stopServices.size());
        List<Exception> exceptions = new ArrayList<Exception>();
        while (reverseIter.hasPrevious()) {
            try {
                Service service = reverseIter.previous();
                //System.out.println("Shutting down service: " + service.getClass()); // TODO change to logging
                service.stop();
            } catch (Exception t) {
                exceptions.add(t);
            }
        }
        if (!exceptions.isEmpty()) {
            if (exceptions.size() == 1) {
                throw exceptions.get(0);
            }
            throw new Exception("Failure(s) while shutting down services: " + exceptions, exceptions.get(0));
        }
    }

    public Service getService(final String name) {
        return services.get(name);
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Services", this, ServiceManagerMXBean.class);
    }

    @Override
    public boolean isStartupBlocked() {
        return blockerLatch.getCount() > 0;
    }

    @Override
    public void resumeStartup() {
        blockerLatch.countDown();
    }

    /**
     * Quick-and-dirty testing tool. Creates a ServiceManager, starts its
     * services (including JMX), and then just sits around waiting to be killed.
     * 
     * @param ignored
     *            ignored
     */
    public static void main(String[] ignored) throws Exception {
        final DefaultServiceManagerFactory serviceManagerFactory = new DefaultServiceManagerFactory();
        ServiceManager sm = serviceManagerFactory.serviceManager();
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
}
