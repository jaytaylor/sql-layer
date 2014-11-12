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

package com.foundationdb.server.service.plugins;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public final class HomeDirPluginsFinder implements PluginsFinder {
    private static final String CONFIG_NAME = "fdbsql.home";

    @Override
    public Collection<? extends Plugin> get() {
        Collection<Plugin> plugins;
        if (!pluginsDir.exists()) {
            plugins = Collections.emptyList();
        }
        else {
            File[] files = pluginsDir.listFiles();
            if (files == null) {
                throw new RuntimeException("'" + pluginsDir + "' must be a directory");
            }
            plugins = new ArrayList<>(files.length);
            for (File pluginJar : files) {
                plugins.add(new JarPlugin(pluginJar));
            }
        }
        return plugins;
    }

    private static final Logger logger = LoggerFactory.getLogger(HomeDirPluginsFinder.class);
    private static final File pluginsDir = findPluginsDir();

    private static File findPluginsDir() {
        String homeDirPath = System.getProperty(CONFIG_NAME);
        if (homeDirPath == null) {
            logger.error("no {} variable set", CONFIG_NAME);
            throw new RuntimeException("no "+CONFIG_NAME+" variable set");
        }
        File homeDir = new File(homeDirPath);
        if (!homeDir.isDirectory()) {
            String msg = "not a directory: " + homeDir.getAbsolutePath();
            logger.error(msg);
            throw new RuntimeException(msg);
        }
        return new File(homeDir, "plugins");
    }

}
