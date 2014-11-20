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

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class Plugin {

    public abstract List<URL> getClassLoaderURLs();
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
