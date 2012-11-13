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

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

public abstract class Plugin {

    public abstract URL getClassLoaderURL();
    public abstract Reader getServiceConfigsReader() throws IOException;
    protected abstract Properties readPropertiesRaw() throws Exception;

    public final Properties readProperties() {
        Properties properties;
        try {
            properties = readPropertiesRaw();
        }
        catch (Exception e) {
            throw new PluginException(e);
        }
        String pluginName = (String) properties.remove("plugins.name");
        if (pluginName == null)
            throw new PluginException("plugin has invalid properties file (missing plugins.name): " + this);
        if (pluginName.contains("."))
            throw new PluginException("plugin has invalid name: " + pluginName);
        Properties results = new Properties();
        StringBuilder keyBuilder = new StringBuilder("plugins.").append(pluginName).append('.');
        final int prefixLength = keyBuilder.length();
        for (Map.Entry<Object, Object> property : properties.entrySet()) {
            String key = string(property.getKey());
            if (key.startsWith("plugins."))
                throw new PluginException("plugin has invalid properties file (key " + key + "): " + this);
            keyBuilder.setLength(prefixLength);
            keyBuilder.append(key);
            results.setProperty(keyBuilder.toString(), string(property.getValue()));
        }
        return results;
    }

    private String string(Object o) {
        if ((o == null) || (o instanceof String))
            return (String) o;
        throw new PluginException("plugin has invalid key or value: " + o + " in " + this);
    }
}
