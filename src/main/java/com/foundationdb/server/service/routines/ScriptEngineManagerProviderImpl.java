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

package com.foundationdb.server.service.routines;

import com.foundationdb.server.error.ServiceStartupException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
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
            // without the rest of the sql layer. But note
            // java.sql.DriverManager.isDriverAllowed(), which requires that a
            // registered driver's class by accessible to its caller by name.
            // May
            // need a JDBCDriver proxy get just to register without putting all
            // of
            // com.foundationdb.sql.embedded into the parent.
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
