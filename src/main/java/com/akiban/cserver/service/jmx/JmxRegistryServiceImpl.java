package com.akiban.cserver.service.jmx;

import com.akiban.cserver.service.Service;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class JmxRegistryServiceImpl implements JmxRegistryService, JmxManageable, Service {
    private static final String FORMATTER = "com.akiban:type=%s";
    private static final String FORMATTER_CONFLICT = "com.akiban:type=%s_%d";

    private boolean started = false;
    private final Map<JmxManageable,ObjectName> serviceToName = new HashMap<JmxManageable, ObjectName>();
    private final Map<ObjectName,JmxManageable> nameToService = new HashMap<ObjectName,JmxManageable>();

    private final Object INTERNAL_LOCK = new Object();

    private void addService(ObjectName objectName, JmxManageable service) {
        assert Thread.holdsLock(INTERNAL_LOCK) : "this method must be called from a synchronized block";

        ObjectName oldName = serviceToName.put(service, objectName);
        JmxManageable oldService = nameToService.put(objectName, service);
        assert oldName == null : String.format("(%s) %s bumped %s", objectName, service, oldName);
        assert oldService == null : String.format("(%s) %s bumped %s", service, objectName, oldService);
    }

    private void removeService(ObjectName objectName) {
        assert Thread.holdsLock(INTERNAL_LOCK) : "this method must be called from a synchronized block";

        JmxManageable removedService = nameToService.remove(objectName);
        assert removedService != null : "removed a null JmxManageble";
        ObjectName removedName = serviceToName.remove(removedService);
        assert removedName.equals(objectName) : String.format("%s != %s", removedName, objectName);
    }

    @Override
    public void register(JmxManageable service) {
        String serviceName = service.getJmxObjectName();
        if (!serviceName.matches("[\\w\\d]+")) {
            throw new JmxRegistrationException(service.getClass(), serviceName);
        }
        final ObjectName objectName;
        synchronized (INTERNAL_LOCK) {
            if (serviceToName.containsKey(service)) {
                throw new JmxRegistrationException("Already registered instance of " + service.getClass());
            }
            objectName = getObjectName(serviceName, nameToService.keySet());
            addService(objectName, service);

            if (started) {
                try {
                    getMBeanServer().registerMBean(service.getJmxObject(), objectName);
                }
                catch (Exception e) {
                    removeService(objectName);
                    throw new JmxRegistrationException(e);
                }
            }
        }
    }

    private MBeanServer getMBeanServer() {
        return ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Returns a unique object name. This method is <em>NOT</em> thread-safe; it's up to the caller to
     * provide any locking for consistency, atomicity, etc. Note that this method does not add the ObjectName
     * it generates to the set.
     * @param serviceName the service name
     * @param uniquenessSet the set that defines existing ObjectNames
     * @return an ObjectName that is not in the given set.
     */
    private ObjectName getObjectName(String serviceName, Set<ObjectName> uniquenessSet) {
        assert Thread.holdsLock(INTERNAL_LOCK) : "this method must be called from a synchronized block";

        ObjectName objectName;
        try {
            objectName = new ObjectName(String.format(FORMATTER, serviceName));
            for(int i=1; uniquenessSet.contains(objectName); ++i) {
                objectName = new ObjectName(String.format(FORMATTER_CONFLICT, serviceName, i));
            }
        } catch (MalformedObjectNameException e) {
            throw new JmxRegistrationException(e);
        }
        return objectName;
    }

    @Override
    public void unregister(String objectNameString) {
        synchronized (INTERNAL_LOCK) {
            try {
                final ObjectName registeredObject = new ObjectName(String.format(FORMATTER, objectNameString));
                getMBeanServer().unregisterMBean(registeredObject);
                removeService(registeredObject);
            } catch (Exception e) {
                throw new JmxRegistrationException(e);
            }
        }
    }

    @Override
    public void start() {
        final MBeanServer mbs = getMBeanServer();
        synchronized (INTERNAL_LOCK) {
            if (started) {
                return;
            }
            for (Map.Entry<JmxManageable,ObjectName> entry : serviceToName.entrySet()) {
                final JmxManageable service = entry.getKey();
                final ObjectName objectName = entry.getValue();
                try {
                    mbs.registerMBean(service.getJmxObject(), objectName);
                } catch (Exception e) {
                    throw new JmxRegistrationException("for " + objectName, e);
                }
            }
            started = true;
        }
    }

    @Override
    public void stop() {
        final MBeanServer mbs = getMBeanServer();
        synchronized (INTERNAL_LOCK) {
            if (!started) {
                return;
            }
            for (ObjectName objectName : nameToService.keySet()) {
                try {
                    mbs.unregisterMBean(objectName);
                } catch (Exception e) {
                    throw new JmxRegistrationException(e);
                }
            }
            started = false;
        }
    }

    @Override
    public String getJmxObjectName() {
        return "JmxManager";
    }

    @Override
    public Object getJmxObject() {
        return this;
    }
}
