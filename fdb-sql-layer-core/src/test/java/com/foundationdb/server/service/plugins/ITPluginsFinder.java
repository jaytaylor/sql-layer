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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ITPluginsFinder implements PluginsFinder
{
    @Override
    public Collection<? extends Plugin> get() {
        return Collections.singletonList(new ITPlugin());
    }

    static class ITPlugin extends Plugin {
        @Override
        public URL getClassLoaderURL() {
            return null;
        }

        @Override
        protected Properties readPropertiesRaw() throws IOException {
            Properties result = new Properties();
            try (InputStream istr = getClass().getResourceAsStream("/" + JarPlugin.PROPERTY_FILE_PATH)) {
                result.load(istr);
            }
            return result;
        }

        @Override
        public Reader getServiceConfigsReader() throws IOException {
            return new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/" + JarPlugin.SERVICE_CONFIG_PATH)));
        }
    }
}
