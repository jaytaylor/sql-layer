/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.servicemanager;

import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.server.error.ServiceStartupException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.jmx.JmxRegistryService;
import com.foundationdb.server.service.servicemanager.configuration.BindingsConfigurationLoader;
import com.foundationdb.server.service.servicemanager.configuration.DefaultServiceConfigurationHandler;
import com.foundationdb.server.service.servicemanager.configuration.ServiceBinding;
import com.foundationdb.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.foundationdb.server.service.servicemanager.configuration.yaml.YamlConfiguration;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.stats.StatisticsService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class GuicedServiceManager implements ServiceManager, JmxManageable {
    // ServiceManager interface

    @Override
    public State getState() {
        return state;
    }

    @Override
    public synchronized void startServices() {
        logger.info("Starting services.");
        state = State.STARTING;
        getJmxRegistryService().register(this);
        boolean ok = false;
        try {
            for (Class<?> directlyRequiredClass : guicer.directlyRequiredClasses()) {
                guicer.get(directlyRequiredClass, STANDARD_SERVICE_ACTIONS);
            }
            ok = true;
        }
        finally {
            if (!ok)
                state = State.ERROR_STARTING;
        }
        state = State.ACTIVE;
        LayerInfoInterface layerInfo = getLayerInfo();
        logger.info("{} {} ready.", layerInfo.getServerName(), layerInfo.getVersionInfo().versionLong);
    }

    @Override
    public synchronized void stopServices() throws Exception {
        logger.info("Stopping services normally.");
        state = State.STOPPING;
        try {
            guicer.stopAllServices(STANDARD_SERVICE_ACTIONS);
        }
        finally {
            state = State.IDLE;
        }
        logger.info("Services stopped.");
    }

    @Override
    public synchronized void crashServices() throws Exception {
        logger.info("Stopping services abnormally.");
        state = State.STOPPING;
        try {
            guicer.stopAllServices(CRASH_SERVICES);
        }
        finally {
            state = State.IDLE;
        }
        logger.info("Services stopped.");
    }

    @Override
    public ConfigurationService getConfigurationService() {
        return getServiceByClass(ConfigurationService.class);
    }

    @Override
    public LayerInfoInterface getLayerInfo() {
        return getServiceByClass(LayerInfoInterface.class);
    }

    @Override
    public Store getStore() {
        return getServiceByClass(Store.class);
    }

    @Override
    public SchemaManager getSchemaManager() {
        return getServiceByClass(SchemaManager.class);
    }

    @Override
    public JmxRegistryService getJmxRegistryService() {
        return getServiceByClass(JmxRegistryService.class);
    }

    @Override
    public StatisticsService getStatisticsService() {
        return getServiceByClass(StatisticsService.class);
    }

    @Override
    public SessionService getSessionService() {
        return getServiceByClass(SessionService.class);
    }

    @Override
    public synchronized <T> T getServiceByClass(Class<T> serviceClass) {
        return guicer.get(serviceClass, STANDARD_SERVICE_ACTIONS);
    }

    @Override
    public DXLService getDXL() {
        return getServiceByClass(DXLService.class);
    }

    @Override
    public MonitorService getMonitorService() {
        return getServiceByClass(MonitorService.class);
    }

    @Override
    public boolean serviceIsBoundTo(Class<?> serviceClass, Class<?> implClass) {
        return guicer.isBoundTo(serviceClass, implClass);
    }

    @Override
    public boolean serviceIsStarted(Class<?> serviceClass) {
        return guicer.serviceIsStarted(serviceClass);
    }

    // JmxManageable interface

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Services", bean, ServiceManagerMXBean.class);
    }


    // GuicedServiceManager interface

    public GuicedServiceManager() {
        this(standardUrls());
    }

    public GuicedServiceManager(BindingsConfigurationProvider bindingsConfigurationProvider) {
        DefaultServiceConfigurationHandler configurationHandler = new DefaultServiceConfigurationHandler();

        // Install the default, no-op JMX registry; this is a special case, since we want to use it
        // as we start each service.
        configurationHandler.bind(JmxRegistryService.class.getName(), NoOpJmxRegistry.class.getName(), null);

        // Next, load each element in the provider...
        for (BindingsConfigurationLoader loader : bindingsConfigurationProvider.loaders()) {
            loader.loadInto(configurationHandler);
        }

        // ... followed by the configured or default file, if either exists
        URL configFile = findServiceConfigFile();
        if (configFile != null) {
            new YamlBindingsUrl(configFile).loadInto(configurationHandler);
        }

        // ... followed by any command-line overrides.
        new PropertyBindings(System.getProperties()).loadInto(configurationHandler);

        Collection<ServiceBinding> bindings = configurationHandler.serviceBindings(true);
        try {
            guicer = Guicer.forServices(ServiceManager.class, this,
                                        bindings, configurationHandler.priorities(), configurationHandler.getModules());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // private methods

    boolean isRequired(Class<?> theClass) {
        return guicer.isRequired(theClass);
    }

    // static methods

    public static BindingsConfigurationProvider standardUrls() {
        BindingsConfigurationProvider provider = new BindingsConfigurationProvider();
        String resourceName = GuicedServiceManager.class.getPackage().getName().replace(".", "/") + "/" + "default-services.yaml";
        Enumeration<URL> e;
        try {
            e = GuicedServiceManager.class.getClassLoader().getResources(resourceName);
        }
        catch (IOException ex) {
            logger.error("while reading services config " + ex);
            throw new ServiceStartupException("error while reading services config");
        }
        while (e.hasMoreElements()) {
            URL source = e.nextElement();
            provider.define(source);
        }
        return provider;
    }

    public static BindingsConfigurationProvider testUrls() {
        BindingsConfigurationProvider provider = standardUrls();
        provider.define(GuicedServiceManager.class.getResource("test-services.yaml"));
        provider.overrideRequires(GuicedServiceManager.class.getResource("test-services-requires.yaml"));
        return provider;
    }

    /**
     * <ul>
     *     <li>
     *         if {@link #SERVICES_CONFIG_PROPERTY} is defined,
     *            return {@link URL}
     *     </li>
     *     <li>
     *         if {@link #CONFIG_DIR_PROPERTY} is defined and
     *         <ul>
     *             <li>contains {@link #DEFAULT_CONFIG_FILE_NAME}, return {@link File}</li>
     *         </ul>
     *     </li>
     *     <li>
     *         return <code>null</code>
     *     </li>
     * </ul>
     */
    private static URL findServiceConfigFile() {
        try {
            String servicesConfigFile = System.getProperty(SERVICES_CONFIG_PROPERTY);
            if(servicesConfigFile != null) {
                return new URL(new File(".").toURI().toURL(), // Default to local file.
                               servicesConfigFile);
            }
            String configDir = System.getProperty(CONFIG_DIR_PROPERTY);
            if(configDir != null) {
                File configFile = new File(configDir, DEFAULT_CONFIG_FILE_NAME);
                if(configFile.isFile()) {
                    return configFile.toURI().toURL();
                }
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("couldn't convert config file to URL", e);
        }
        return null;
    }

    // object state

    private State state = State.IDLE;
    private final Guicer guicer;

    private final ServiceManagerMXBean bean = new ServiceManagerMXBean() {
        @Override
        public List<String> getStartedDependencies() {
            boolean fullNames = isFullClassNames();
            List<String> result = new ArrayList<>();
            for (Class<?> requiredClass : guicer.directlyRequiredClasses()) {
                List<?> dependencies = guicer.dependenciesFor(requiredClass);
                List<String> dependenciesClasses = new ArrayList<>();
                for (Object dependency : dependencies) {
                    Class<?> depClass = dependency.getClass();
                    dependenciesClasses.add(fullNames ? depClass.getName() : depClass.getSimpleName());
                }
                result.add(dependenciesClasses.toString());
            }
            return result;
        }

        @Override
        public boolean isFullClassNames() {
            return fullClassNames.get();
        }

        @Override
        public void setFullClassNames(boolean value) {
            fullClassNames.set(value);
        }

        @Override
        public List<String> getServicesInStartupOrder() {
            List<String> result = new ArrayList<>();
            for (Class<?> serviceClass : guicer.servicesClassesInStartupOrder()) {
                result.add(isFullClassNames() ? serviceClass.getName() : serviceClass.getSimpleName() );
            }
            return result;
        }

        private final AtomicBoolean fullClassNames = new AtomicBoolean(false);
    };

    final Guicer.ServiceLifecycleActions<Service> STANDARD_SERVICE_ACTIONS
            = new Guicer.ServiceLifecycleActions<Service>()
    {
        private Map<Class<? extends JmxManageable>,ObjectName> jmxNames
                = Collections.synchronizedMap(new HashMap<Class<? extends JmxManageable>, ObjectName>());

        @Override
        public void onStart(Service service) {
            final Thread currentThread = Thread.currentThread();
            final ClassLoader oldContextCl = currentThread.getContextClassLoader();
            ClassLoader contextClassloader = service.getClass().getClassLoader();
            boolean setContextCl = (contextClassloader != null && contextClassloader != oldContextCl);
            try {
                if (setContextCl)
                    currentThread.setContextClassLoader(contextClassloader);
                service.start();
                if (service instanceof JmxManageable && isRequired(JmxRegistryService.class)) {
                    JmxRegistryService registry = (service instanceof JmxRegistryService)
                            ? (JmxRegistryService) service
                            : getJmxRegistryService();
                    JmxManageable manageable = (JmxManageable)service;
                    ObjectName objectName = registry.register(manageable);
                    jmxNames.put(manageable.getClass(), objectName);
                    // TODO because our dependency graph is created via Service.start() invocations, if service A uses service B
                    // in stop() but not start(), and service B has already been shut down, service B will be resurrected. Yuck.
                    // I don't know of a good way around this, other than by formalizing our dependency graph via constructor
                    // params (and thus removing ServiceManagerImpl.get() ). Until this is resolved, simplest is to just shrug
                    // our shoulders and not check
    //                assert (ObjectName)old == null : objectName + " has displaced " + old;
                }
            }
            finally {
                if (setContextCl)
                    currentThread.setContextClassLoader(oldContextCl);
            }
        }

        @Override
        public void onShutdown(Service service) {
            if (service instanceof JmxManageable && isRequired(JmxRegistryService.class)) {
                JmxRegistryService registry = (service instanceof JmxRegistryService)
                        ? (JmxRegistryService) service
                        : getJmxRegistryService();
                JmxManageable manageable = (JmxManageable) service;
                ObjectName objectName = jmxNames.get(manageable.getClass());
                if (objectName == null) {
                    throw new NullPointerException("service not registered: " + manageable.getClass());
                }
                registry.unregister(objectName);
            }
            service.stop();
        }

        @Override
        public Service castIfActionable(Object object) {
            return (object instanceof Service) ? (Service)object : null;
        }
    };

    // consts

    private static final String CONFIG_DIR_PROPERTY = "fdbsql.config_dir"; // Note: ConfigurationServiceImpl
    private static final String DEFAULT_CONFIG_FILE_NAME = "services-config.yaml";
    private static final String SERVICES_CONFIG_PROPERTY = "services.config";

    private static final Guicer.ServiceLifecycleActions<Service> CRASH_SERVICES
            = new Guicer.ServiceLifecycleActions<Service>() {
        @Override
        public void onStart(Service service) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onShutdown(Service service){
            service.crash();
        }

        @Override
        public Service castIfActionable(Object object) {
            return (object instanceof Service) ? (Service) object : null;
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(GuicedServiceManager.class);

    // nested classes

    /**
     * Definition of URLs to use for defining service bindings. There are two sections of URls: the defines
     * and requires. You can have as many defines as you want, but only one requires. When parsing the resources,
     * the defines will be processed (in order) before the requires resource.
     */
    public static final class BindingsConfigurationProvider {

        // BindingsConfigurationProvider interface

        /**
         * Adds a URL to the the internal list.
         * @param url the url to add
         * @return this instance; useful for chaining
         */
        public BindingsConfigurationProvider define(URL url) {
            elements.add(new YamlBindingsUrl(url));
            return this;
        }

        /**
         * Adds a service binding to the internal list. This is equivalent to a yaml segment of
         * {@code bind: {theInteface : theImplementation}}. For instance, it does not affect locking, and if the
         * interface is locked, this will fail at run time.
         * @param anInterface the interface to bind to
         * @param anImplementation the implementing class
         * @param <T> the interface's type
         * @return this instance; useful for chaining
         */
        public <T> BindingsConfigurationProvider bind(Class<T> anInterface, Class<? extends T> anImplementation) {
            elements.add(new ManualServiceBinding(anInterface.getName(), anImplementation.getName(), false));
            return this;
        }

        /**
         * Adds a service binding to the internal list. This is equivalent to a yaml segment of
         * {@code bind: {theInteface : theImplementation}}. For instance, it does not affect locking, and if the
         * interface is locked, this will fail at run time.
         * @param anInterface the interface to bind to
         * @param anImplementation the implementing class
         * @param <T> the interface's type
         * @return this instance; useful for chaining
         */
        public <T> BindingsConfigurationProvider bindAndRequire(Class<T> anInterface, Class<? extends T> anImplementation) {
            elements.add(new ManualServiceBinding(anInterface.getName(), anImplementation.getName(), true));
            return this;
        }

        /**
         * Adds a service requirement to the internal list. This is equivalent to a yaml segment of
         * {@code require: {theInteface}}.
         * @param anInterface the interface to require
         * @param <T> the interface's type
         * @return this instance; useful for chaining
         */
        public <T> BindingsConfigurationProvider require(Class<T> anInterface) {
            elements.add(new ManualServiceBinding(anInterface.getName()));
            return this;
        }

        /**
         * Overrides the "requires" section of the URL definitions. This replaces the old requires URL.
         * @param url the new requires URL
         * @return this instance; useful for chaining
         */
        public BindingsConfigurationProvider overrideRequires(URL url) {
            requires = url;
            return this;
        }

        // for use in this package

        public Collection<BindingsConfigurationLoader> loaders() {
            List<BindingsConfigurationLoader> urls = new ArrayList<>(elements);
            if (requires != null) {
                urls.add(new YamlBindingsUrl(requires));
            }
            return urls;
        }


        // object state

        private final List<BindingsConfigurationLoader> elements = new ArrayList<>();
        private URL requires = null;
    }

    private static class YamlBindingsUrl implements BindingsConfigurationLoader {
        @Override
        public void loadInto(ServiceConfigurationHandler config) {
            final InputStream defaultServicesStream;
            try {
                defaultServicesStream = url.openStream();
            } catch(IOException e) {
                throw new RuntimeException("no resource " + url, e);
            }
            final Reader defaultServicesReader;
            try {
                defaultServicesReader = new InputStreamReader(defaultServicesStream, "UTF-8");
            } catch (Exception e) {
                try {
                    defaultServicesStream.close();
                } catch (IOException ioe) {
                    LOG.error("while closing stream error", ioe);
                }
                throw new RuntimeException("while opening default services reader", e);
            }
            RuntimeException exception = null;
            try {
                new YamlConfiguration(url.toString(), defaultServicesReader, null).loadInto(config);
            } catch (RuntimeException e) {
                exception = e;
            } finally {
                try {
                    defaultServicesReader.close();
                } catch (IOException e) {
                    if (exception == null) {
                        exception = new RuntimeException("while closing " + url, e);
                    }
                    else {
                        LOG.error("while closing url after exception " + exception, e);
                    }
                }
            }
            if (exception != null) {
                throw exception;
            }
        }

        private YamlBindingsUrl(URL url) {
            this.url = url;
        }

        private final URL url;
    }

    private static final BindingsConfigurationLoader emptyConfigurationLoader = new BindingsConfigurationLoader() {
        @Override
        public void loadInto(ServiceConfigurationHandler config) {}
    };

    private static class ManualServiceBinding implements BindingsConfigurationLoader {

        // BindingsConfigurationElement interface

        @Override
        public void loadInto(ServiceConfigurationHandler config) {
            if (implementationName != null)
                config.bind(interfaceName, implementationName, null);
            if (required)
                config.require(interfaceName);
        }


        // ManualServiceBinding interface

        private ManualServiceBinding(String interfaceName, String implementationName, boolean required) {
            this.interfaceName = interfaceName;
            this.implementationName = implementationName;
            this.required = required;
        }

        private ManualServiceBinding(String interfaceName) {
            this(interfaceName, null, true);
        }

        // object state

        private final String interfaceName;
        private final String implementationName;
        private final boolean required;
    }

    static class PropertyBindings implements BindingsConfigurationLoader {
        // BindingsConfigurationElement interface

        @Override
        public void loadInto(ServiceConfigurationHandler config) {
            for (String property : properties.stringPropertyNames()) {
                if (property.startsWith(BIND)) {
                    String theInterface = property.substring(BIND.length());
                    String theImpl = properties.getProperty(property);
                    if (theInterface.length() == 0) {
                        throw new IllegalArgumentException("empty -Dbind: property found");
                    }
                    if (theImpl.length() == 0) {
                        throw new IllegalArgumentException("-D" + property + " doesn't have a valid value");
                    }
                    config.bind(theInterface, theImpl, null);
                } else if (property.startsWith(REQUIRE)) {
                    String theInterface = property.substring(REQUIRE.length());
                    String value = properties.getProperty(property);
                    if (value.length() != 0) {
                        throw new IllegalArgumentException(
                                String.format("-Drequire tags may not have values: %s = %s", theInterface, value)
                        );
                    }
                    config.require(theInterface);
                } else if (property.startsWith(PRIORITIZE)) {
                    String theInterface = property.substring(PRIORITIZE.length());
                    String value = properties.getProperty(property);
                    if (value.length() != 0) {
                        throw new IllegalArgumentException(
                                String.format("-Dprioritize tags may not have values: %s = %s", theInterface, value)
                        );
                    }
                    config.prioritize(theInterface);
                }
            }
        }

        // PropertyBindings interface

        PropertyBindings(Properties properties) {
            this.properties = properties;
        }

        // for use in unit tests

        // object state

        private final Properties properties;

        // consts

        private static final String BIND = "bind:";
        private static final String REQUIRE = "require:";
        private static final String PRIORITIZE = "prioritize:";
    }

    public static class NoOpJmxRegistry implements JmxRegistryService {
        @Override
        public ObjectName register(JmxManageable service) {
            try {
                return new ObjectName("com.foundationdb:type=DummyPlaceholder" + counter.incrementAndGet());
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void unregister(ObjectName registeredObject) {
        }

        @Override
        public void unregister(String serviceName) {
        }

        // object state
        private final AtomicInteger counter = new AtomicInteger();
    }
}
