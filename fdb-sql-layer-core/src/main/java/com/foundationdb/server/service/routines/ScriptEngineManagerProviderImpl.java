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

public final class ScriptEngineManagerProviderImpl implements ScriptEngineManagerProvider, Service {
    @Override
    public ScriptEngineManager getManager() {
        return manager;
    }

    @Override
    public ClassLoader getSafeClassLoader() {
        return safeClassLoader;
    }
    
    @Override
    public void start() {
        // TODO: The idea should be to restrict scripts to standard Java
        // classes without the rest of the sql layer. But note
        // java.sql.DriverManager.isDriverAllowed(), which requires that a
        // registered driver's class by accessible to its caller by name.
        // May need a JDBCDriver proxy just to register without putting all
        // of com.foundationdb.sql.embedded into the parent.
        ClassLoader parentClassLoader = getClass().getClassLoader().getParent();

        String classPath = configService.getProperty(CLASS_PATH);
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
        safeClassLoader = new URLClassLoader(urls, parentClassLoader);
        
        manager = new ScriptEngineManager(safeClassLoader);
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
    private ScriptEngineManager manager;
    private ClassLoader safeClassLoader;

    private static final Logger logger = LoggerFactory.getLogger(ScriptEngineManagerProviderImpl.class);

}
