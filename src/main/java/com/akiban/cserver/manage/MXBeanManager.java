package com.akiban.cserver.manage;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerConfig;

public class MXBeanManager
{
    private static boolean registered;
    
    public synchronized static void registerMXBean(CServer cserver, CServerConfig config) throws Exception
    {
        if (registered) {
            return;
        }

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        ObjectName mxbeanName = new ObjectName(ManageMXBean.MANAGE_BEAN_NAME);
        mbs.registerMBean(new ManageMXBeanImpl(cserver, config), mxbeanName);

        ObjectName schemaMxbeanName = new ObjectName(SchemaMXBean.SCHEMA_BEAN_NAME);
        mbs.registerMBean(new SchemaMXBeanImpl(cserver), schemaMxbeanName);
        
        registered = true;
    }

    public synchronized static void unregisterMXBean() throws Exception
    {
        if (!registered) {
            return;
        }

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        ObjectName mxbeanName = new ObjectName(ManageMXBean.MANAGE_BEAN_NAME);
        mbs.unregisterMBean(mxbeanName);

        ObjectName schemaMxbeanName = new ObjectName(SchemaMXBean.SCHEMA_BEAN_NAME);
        mbs.unregisterMBean(schemaMxbeanName);

        registered = false;
    }
}
