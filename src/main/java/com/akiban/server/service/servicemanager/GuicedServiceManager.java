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

package com.akiban.server.service.servicemanager;

import com.akiban.server.AkServerInterface;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManager.State;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.instrumentation.InstrumentationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.servicemanager.configuration.BindingsConfigurationLoader;
import com.akiban.server.service.servicemanager.configuration.DefaultServiceConfigurationHandler;
import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.akiban.server.service.servicemanager.configuration.ServiceConfigurationHandler;
import com.akiban.server.service.servicemanager.configuration.yaml.YamlConfiguration;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.sql.pg.PostgresService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
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
    public void startServices() {
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
        AkServerInterface akServer = getAkSserver();
        logger.info("{} {} ready.",
                    akServer.getServerName(), akServer.getServerVersion());
    }

    @Override
    public void stopServices() throws Exception {
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
    public void crashServices() throws Exception {
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
    public AkServerInterface getAkSserver() {
        return getServiceByClass(AkServerInterface.class);
    }

    @Override
    public Store getStore() {
        return getServiceByClass(Store.class);
    }

    @Override
    public TreeService getTreeService() {
        return getServiceByClass(TreeService.class);
    }

    @Override
    public PostgresService getPostgresService() {
        return getServiceByClass(PostgresService.class);
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
    public <T> T getServiceByClass(Class<T> serviceClass) {
        return guicer.get(serviceClass, STANDARD_SERVICE_ACTIONS);
    }

    @Override
    public DXLService getDXL() {
        return getServiceByClass(DXLService.class);
    }

    @Override
    public InstrumentationService getInstrumentationService() {
        return getServiceByClass(InstrumentationService.class);
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

    public GuicedServiceManager(BindingsConfigurationProvider bindingsConfigurationProvider) {
        DefaultServiceConfigurationHandler configurationHandler = new DefaultServiceConfigurationHandler();

        // Install the default, no-op JMX registry; this is a special case, since we want to use it
        // as we start each service.
        configurationHandler.bind(JmxRegistryService.class.getName(), NoOpJmxRegistry.class.getName());

        // Next, load each element in the provider...
        for (BindingsConfigurationLoader loader : bindingsConfigurationProvider.loaders()) {
            loader.loadInto(configurationHandler);
        }

        // ... followed by whatever is in the file specified by -Dservices.config=blah, if that's defined...
        String configFileName = System.getProperty(SERVICES_CONFIG_PROPERTY);
        if (configFileName != null) {
            File configFile = new File(configFileName);
            if (!configFile.isFile()) {
                throw new RuntimeException("file not found or isn't a normal file: " + configFileName);
            }
            final URL configFileUrl;
            try {
                configFileUrl = configFile.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException("couldn't convert config file to URL", e);
            }
            new YamlBindingsUrl(configFileUrl).loadInto(configurationHandler);
        }

        // ... followed by any command-line overrides.
        new PropertyBindings(System.getProperties()).loadInto(configurationHandler);

        final Collection<ServiceBinding> bindings = configurationHandler.serviceBindings();
        try {
            guicer = Guicer.forServices(ServiceManager.class, this, 
                                        bindings, configurationHandler.priorities());
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
        provider.define(GuicedServiceManager.class.getResource("default-services.yaml"));
        return provider;
    }

    public static BindingsConfigurationProvider testUrls() {
        BindingsConfigurationProvider provider = standardUrls();
        provider.define(GuicedServiceManager.class.getResource("test-services.yaml"));
        provider.overrideRequires(GuicedServiceManager.class.getResource("test-services-requires.yaml"));
        return provider;
    }

    // object state

    private State state = State.IDLE;
    private final Guicer guicer;

    private final ServiceManagerMXBean bean = new ServiceManagerMXBean() {
        @Override
        public List<String> getStartedDependencies() {
            boolean fullNames = isFullClassNames();
            List<String> result = new ArrayList<String>();
            for (Class<?> requiredClass : guicer.directlyRequiredClasses()) {
                List<?> dependencies = guicer.dependenciesFor(requiredClass);
                List<String> dependenciesClasses = new ArrayList<String>();
                for (Object dependency : dependencies) {
                    Class<?> depClass = dependency.getClass();
                    dependenciesClasses.add(fullNames ? depClass.getName() : depClass.getSimpleName());
                }
                result.add(dependenciesClasses.toString());
            }
            return result;
        }

        @Override
        public void graphStartedDependencies(String filename) {
            guicer.graph(filename, guicer.directlyRequiredClasses());
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
            List<String> result = new ArrayList<String>();
            for (Class<?> serviceClass : guicer.servicesClassesInStartupOrder()) {
                result.add(isFullClassNames() ? serviceClass.getName() : serviceClass.getSimpleName() );
            }
            return result;
        }

        private final AtomicBoolean fullClassNames = new AtomicBoolean(false);
    };

    final Guicer.ServiceLifecycleActions<Service<?>> STANDARD_SERVICE_ACTIONS
            = new Guicer.ServiceLifecycleActions<Service<?>>()
    {
        private Map<Class<? extends JmxManageable>,ObjectName> jmxNames
                = Collections.synchronizedMap(new HashMap<Class<? extends JmxManageable>, ObjectName>());

        @Override
        public void onStart(Service<?> service) {
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

        @Override
        public void onShutdown(Service<?> service) {
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
        public Service<?> castIfActionable(Object object) {
            return (object instanceof Service) ? (Service<?>)object : null;
        }
    };

    // consts

    private static final String SERVICES_CONFIG_PROPERTY = "services.config";

    private static final Guicer.ServiceLifecycleActions<Service<?>> CRASH_SERVICES
            = new Guicer.ServiceLifecycleActions<Service<?>>() {
        @Override
        public void onStart(Service<?> service) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onShutdown(Service<?> service){
            service.crash();
        }

        @Override
        public Service<?> castIfActionable(Object object) {
            return (object instanceof Service) ? (Service<?>) object : null;
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
            elements.add(new ManualServiceBinding(anInterface.getName(), anImplementation.getName()));
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
            List<BindingsConfigurationLoader> urls = new ArrayList<BindingsConfigurationLoader>(elements);
            if (requires != null) {
                urls.add(new YamlBindingsUrl(requires));
            }
            return urls;
        }


        // object state

        private final List<BindingsConfigurationLoader> elements = new ArrayList<BindingsConfigurationLoader>();
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
                new YamlConfiguration(url.toString(), defaultServicesReader).loadInto(config);
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

    private static class ManualServiceBinding implements BindingsConfigurationLoader {

        // BindingsConfigurationElement interface

        @Override
        public void loadInto(ServiceConfigurationHandler config) {
            config.bind(interfaceName, implementationName);
        }


        // ManualServiceBinding interface

        private ManualServiceBinding(String interfaceName, String implementationName) {
            this.interfaceName = interfaceName;
            this.implementationName = implementationName;
        }

        // object state

        private final String interfaceName;
        private final String implementationName;
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
                    config.bind(theInterface, theImpl);
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
                return new ObjectName("com.akiban:type=DummyPlaceholder" + counter.incrementAndGet());
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
