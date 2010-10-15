package com.akiban.cserver.service;

import java.util.*;

import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.jmx.JmxRegistryServiceImpl;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.network.NetworkServiceImpl;

public class ServiceManager
{
    private static final ServiceManager instance = new ServiceManager();
    private Map<String, Service> services;
    private static final String NETWORK = "network";
    private static final String CONFIGURATION = "configuration";
    private static final String JMX = "jmx";
    
    public static ServiceManager get()
    {
        return instance;
    }

    private ServiceManager()
    {
        services = new LinkedHashMap<String, Service>();
    }

    public void startServices()
    {
        JmxRegistryServiceImpl jmxRegistry = new JmxRegistryServiceImpl();
        
        startAndPut(new ConfigurationServiceImpl(), CONFIGURATION);
        startAndPut(new NetworkServiceImpl(), NETWORK);

        for (Service service : services.values()) {
            if (service instanceof JmxManageable) {
                jmxRegistry.register( (JmxManageable) service );

            }
        }
        startAndPut(jmxRegistry, JMX);
    }

    private void startAndPut(Service service, String name) {
        service.start();
        services.put(name, service);
    }
    
    public void stopServices()
    {
        List<Service> stopServices = new ArrayList<Service>(services.size());
        for (Service service : services.values()) {
            stopServices.add(service);
        }
        ListIterator<Service> reverseIter = stopServices.listIterator(stopServices.size());
        while (reverseIter.hasPrevious()) {
            reverseIter.previous().stop();
        }
    }

    /**
     * Quick-and-dirty testing tool. Creates a ServiceManager, starts its services (including JMX),
     * and then just sits around waiting to be killed. 
     * @param ignored ignored
     */
    public static void main(String[] ignored) {
        ServiceManager sm = new ServiceManager();
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
