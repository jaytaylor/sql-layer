package com.akiban.cserver.service.jmx;

import com.akiban.util.ArgumentValidation;

public interface JmxManageable {
    public JmxObjectInfo getJmxObjectInfo();

    public static class JmxObjectInfo {
        private final Object instance;
        private final String objectName;
        private final Class<?> jmxInterface;

        public JmxObjectInfo(String objectName, Object instance, Class<?> jmxInterface) {
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
