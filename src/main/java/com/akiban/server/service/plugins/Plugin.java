
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
