
package com.akiban.server.service.plugins;

import com.akiban.server.error.ServiceStartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public final class HomeDirPluginsFinder implements PluginsFinder {

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
        String homeDirPath = System.getProperty("akiban.home");
        if (homeDirPath == null) {
            logger.error("no akiban.home variable set");
            throw new RuntimeException("no akiban.home variable set");
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
