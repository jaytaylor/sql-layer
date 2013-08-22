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

package com.foundationdb.server.service.jmx;

import com.foundationdb.util.ArgumentValidation;

public interface JmxManageable {
    public JmxObjectInfo getJmxObjectInfo();

    public static class JmxObjectInfo {
        private final Object instance;
        private final String objectName;
        private final Class<?> jmxInterface;

        public <T> JmxObjectInfo(String objectName, T instance, Class<T> jmxInterface) {
            ArgumentValidation.notNull("JMX object instance", instance);
            ArgumentValidation.notNull("object name", objectName);
            ArgumentValidation.notNull("interfaces", jmxInterface);

            this.instance = instance;
            this.objectName = objectName;
            this.jmxInterface = jmxInterface;
        }

        public Object getInstance() {
            return instance;
        }

        public String getObjectName() {
            return objectName;
        }

        public Class<?> getManagedInterface() {
            return jmxInterface;
        }
    }
}
