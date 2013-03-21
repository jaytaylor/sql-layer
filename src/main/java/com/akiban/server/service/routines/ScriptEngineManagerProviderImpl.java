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

package com.akiban.server.service.routines;

import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngineManager;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReference;

public final class ScriptEngineManagerProviderImpl implements ScriptEngineManagerProvider, Service {
    @Override
    public ScriptEngineManager getManager() {
        ScriptEngineManager r = manager.get();
        if (r == null)
            throw new IllegalStateException("service not started");
        return r;
    }

    @Override
    public void start() {
        manager = new ThreadLocalScriptEngineManager();
    }

    @Override
    public void stop() {
        manager = null;
    }

    @Override
    public void crash() {
        stop();
    }

    @Inject @SuppressWarnings("unused")
    public ScriptEngineManagerProviderImpl(ConfigurationService configService) {
        this.configService = configService;
    }

    private final ConfigurationService configService;
    private volatile ThreadLocalScriptEngineManager manager;

    private class ThreadLocalScriptEngineManager extends ThreadLocal<ScriptEngineManager>{
        @Override
        protected ScriptEngineManager initialValue() {
            logger.debug("Initializing script engine manager");
            String classPath = configService.getProperty(CLASS_PATH);
            // TODO: The idea should be to restrict scripts to standard Java
            // classes
            // without the rest of the Akiban server. But note
            // java.sql.DriverManager.isDriverAllowed(), which requires that a
            // registered driver's class by accessible to its caller by name.
            // May
            // need a JDBCDriver proxy get just to register without putting all
            // of
            // com.akiban.sql.embedded into the parent.
            String[] paths = classPath.split(File.pathSeparator);
            URL[] urls = new URL[paths.length];
            try {
                for (int i = 0; i < paths.length; i++) {
                    urls[i] = new File(paths[i]).toURI().toURL();
                }
            } catch (MalformedURLException ex) {
                logger.warn("Error setting script class loader", ex);
                urls = new URL[0];
            }
            ClassLoader classLoader = new ScriptClassLoader(urls, getClass().getClassLoader());
            return new ScriptEngineManager(classLoader);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ScriptEngineManagerProviderImpl.class);

    /**
     * Extended URLClassLoader that uses a thread-private context class loader
     * to load generated classes. There is one ScriptClassLoader per
     * ScriptEngineManager.
     */
    static class ScriptClassLoader extends URLClassLoader {


        public ScriptClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {

                Class<?> cl = findLoadedClass(name);
                if (cl == null) {
                    try {
                        // delegate to parent
                        cl = getParent().loadClass(name);
                    } catch (ClassNotFoundException e1) {
                        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
                        if (contextLoader != this) {
                            try {
                                // Attempt to load generated class
                                cl = contextLoader.loadClass(name);
                            } catch (ClassNotFoundException e2) {
                                // fall through
                            }
                        }
                    }
                }
                if (cl == null) {
                    cl = findClass(name);
                }
                if (resolve) {
                    resolveClass(cl);
                }
                return cl;
            }
        }
    }
}
