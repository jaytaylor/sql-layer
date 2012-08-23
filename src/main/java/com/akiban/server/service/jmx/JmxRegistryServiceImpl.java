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

package com.akiban.server.service.jmx;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.akiban.server.service.Service;

public class JmxRegistryServiceImpl implements JmxRegistryService, JmxManageable, Service<JmxRegistryService> {
    private static final String FORMATTER = "com.akiban:type=%s";

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
    public ObjectName register(JmxManageable service) {
        final JmxObjectInfo info = service.getJmxObjectInfo();
        validate(info);
        String serviceName = info.getObjectName();
        if (!serviceName.matches("[\\w-]+")) {
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
                    getMBeanServer().registerMBean(info.getInstance(), objectName);
                    return objectName;
                }
                catch (Exception e) {
                    removeService(objectName);
                    throw new JmxRegistrationException(e);
                }
            }
            else {
                return objectName;
            }
        }
    }

    protected MBeanServer getMBeanServer() {
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
            if (uniquenessSet.contains(objectName)) {
                throw new JmxRegistrationException("Bean name conflict: " + serviceName);
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
                if (started) {
                    getMBeanServer().unregisterMBean(registeredObject);
                }
                removeService(registeredObject);
            } catch (Exception e) {
                throw new JmxRegistrationException(e);
            }
        }
    }

    @Override
    public void unregister(ObjectName registeredObject) {
        synchronized (INTERNAL_LOCK) {
            try {
                if (started) {
                    getMBeanServer().unregisterMBean(registeredObject);
                }
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
                    mbs.registerMBean(service.getJmxObjectInfo().getInstance(), objectName);
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
    public void crash() {
        // Nice to unregister these so that a restarted instance can
        // register new instances.
        stop();
    }
    


    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("JmxManager", this, JmxRegistryServiceMXBean.class);
    }

    private static boolean isManagable(Class<?> theInterface) {
        return theInterface.getSimpleName().endsWith("MXBean");
    }

    void validate(JmxObjectInfo objectInfo) {
        Class<?> jmxInterface = objectInfo.getManagedInterface();
        if (!isManagable(jmxInterface)) {
            throw new JmxRegistrationException("Managed interface must end in \"MXBean\"");
        }
        assert jmxInterface.isAssignableFrom(objectInfo.getInstance().getClass())
            : String.format("%s is not assignable from %s", jmxInterface, objectInfo.getInstance().getClass());
        Set<Class<?>> objectInterfaces = getAllInterfaces(objectInfo.getInstance().getClass(), new HashSet<Class<?>>());
        Iterator<Class<?>> interfacesIter = objectInterfaces.iterator();
        while (interfacesIter.hasNext()) {
            if (!isManagable(interfacesIter.next())) {
                interfacesIter.remove();
            }
        }
        if (objectInterfaces.size() != 1) {
            throw new JmxRegistrationException("Need exactly one *MXBean interface for "
                    + objectInfo.getInstance().getClass() + ". Found: " + objectInterfaces);
        }
    }

    private static Set<Class<?>> getAllInterfaces(Class<?> root, Set<Class<?>> set) {
        if (root.isInterface()) {
            set.add(root);
        }
        getAllInterfaces(root.getInterfaces(), set);
        Class<?> rootSuper = root.getSuperclass();
        if (rootSuper != null) {
            getAllInterfaces(rootSuper, set);
        }
        return set;
    }

    private static void getAllInterfaces(Class<?>[] roots, Set<Class<?>> set) {
        for (Class<?> root : roots) {
            assert root.isInterface() : root;
            if (!set.contains(root)) {
                set.add(root);
                getAllInterfaces(root.getInterfaces(), set);
            }
        }
    }
}
