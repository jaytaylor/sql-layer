package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.logging.LoggingServiceImpl;
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
    private Map<Class<?>, Service<?>> services; // for each key-val, the ? should be the same; (T.class -> Service<T>)

    private final CountDownLatch blockerLatch = new CountDownLatch(1);

    public static void setServiceManager(ServiceManager newInstance)
    {
        if (newInstance == null) {
            instance.set(null);
        }
        else if (!instance.compareAndSet(null, newInstance)) {
            throw new RuntimeException("Tried to install a ServiceManager, but one was already set");
        }
    }

    /**
     * This constructor is made protected for unit testing.
     * @param factory the factory that creates the services this instance manages
     */
    protected ServiceManagerImpl(ServiceManagerFactory factory)
    {
        this.factory = factory;
        services = new LinkedHashMap<Class<?>, Service<?>>();
    }

    public static ServiceManager get()
    {
        return instance.get();
    }

    @Override
    public CServer getCServer() {
        return getService(CServer.class);
    }

    @Override
    public Store getStore() {
        return getService(Store.class);
    }

    @Override
    public LoggingService getLogging() {
        return getService(LoggingService.class);
    }

    @Override
    public SessionService getSessionService() {
        return getService(SessionService.class);
    }

    public void startServices() throws Exception {

        JmxRegistryServiceImpl jmxRegistry = new JmxRegistryServiceImpl();
        startAndPut(jmxRegistry, jmxRegistry);
        startAndPut(factory.configurationService(), jmxRegistry);

        jmxRegistry.register(this);
        ConfigurationService configService = getServiceAsService(ConfigurationService.class).cast();
        servicesDebugHooks(configService);

        // TODO: CServerConfig setup is still a mess. Clean up and move to DefaultServiceManagerFactory.
        startAndPut(new LoggingServiceImpl(), jmxRegistry);
        startAndPut(new SessionServiceImpl(), jmxRegistry);
        startAndPut(factory.storeService(), jmxRegistry);
        startAndPut(factory.networkService(), jmxRegistry);
        startAndPut(factory.chunkserverService(), jmxRegistry);
        startAndPut(new SchemaServiceImpl( factory.storeService().cast().getSchemaManager() ), jmxRegistry);
        startAndPut(factory.memcacheService(), jmxRegistry);
        setServiceManager(this);
    }

    private void servicesDebugHooks(ConfigurationService configService)
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

    private void startAndPut(Service service, JmxRegistryService jmxRegistry) throws Exception {
        //System.out.println("Starting service: " + service.getClass()); // TODO change to logging
        service.start();
        Service<?> old = services.put(service.castClass(), service);
        if (old != null) {
            services.put(service.castClass(), old);
            throw new RuntimeException(String.format("Conflicting services: %s (%s) would bump %s (%s)",
                    service.getClass(), service.castClass(), old.getClass(), old.castClass()));
        }
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

    private <T> T getService(Class<T> ofClass) {
        Service<T> serviceT = getServiceAsService(ofClass);
        if (serviceT == null) {
            throw new ServiceNotStartedException(ofClass.getName());
        }
        return serviceT.cast();
    }

    private <T> Service<T> getServiceAsService(Class<T> ofClass) {
        final Service<?> service = services.get(ofClass);
        if (service == null) {
            return null;
        }
        final Object asObject = service.cast();
        if (!ofClass.isInstance(asObject)) {
            Class<?> actualClass = asObject == null ? null : asObject.getClass();
            throw new RuntimeException(
                    String.format("%s expected to be of class %s, was %s", asObject, ofClass, actualClass));
        }
        @SuppressWarnings("unchecked") final Service<T> serviceT = (Service<T>) service;
        assert serviceT.castClass().equals(ofClass) : String.format("%s != %s", serviceT.castClass(), ofClass);
        return serviceT;
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
