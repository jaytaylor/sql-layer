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
            plugins = new ArrayList<Plugin>(files.length);
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
