/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.cserver.service.jmx;

import com.akiban.util.ArgumentValidation;

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
