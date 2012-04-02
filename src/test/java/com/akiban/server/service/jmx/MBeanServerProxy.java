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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.akiban.util.ArgumentValidation;

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
