package com.akiban.cserver.service.jmx;

import com.akiban.util.ArgumentValidation;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;

public final class MBeanServerProxy {

    public interface MockMBeanServer extends MBeanServer {
        Set<ObjectName> getRegisteredObjectNames();
    }

    public static MockMBeanServer getMock() {
        return (MockMBeanServer) Proxy.newProxyInstance(
                MockMBeanServer.class.getClassLoader(),
                new Class[] {MockMBeanServer.class},
                new MBeanServerHandler()
        );
    }

    private static class MBeanServerHandler implements InvocationHandler {

        private final Map<ObjectName,Object> objects = new HashMap<ObjectName,Object>();

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("registerMBean".equals(method.getName())) {
                ArgumentValidation.arrayLength("args", args, 2);
                ArgumentValidation.notNull("object", args[0]);
                ArgumentValidation.notNull("objectName", args[1]);
                objects.put((ObjectName)args[1], args[0]);
                return new ObjectInstance((ObjectName)args[1], args[0].getClass().getCanonicalName());
            }
            else if ("unregisterMBean".equals(method.getName())) {
                ArgumentValidation.arrayLength("args", args, 1);
                ArgumentValidation.notNull("objectName", args[0]);
                Object prev = objects.remove(args[0]);
                if (prev == null) {
                    throw new InstanceNotFoundException(args[0].toString());
                }
                return null;
            }
            else if ("getRegisteredObjectNames".equals(method.getName())) {
                ArgumentValidation.isNull("args", args);
                return Collections.unmodifiableSet(objects.keySet());
            }
            else {
                throw new UnsupportedOperationException(method.toString());
            }
        }
    }
}
