package com.akiban.cserver.manage;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.akiban.cserver.CServerConfig;

public class MXBeanManager
{
    private static boolean registered;
    
    public synchronized static void registerMXBean(CServerConfig config) throws Exception
    {
        if (registered) {
            return;
        }
        
        ObjectName mxbeanName = new ObjectName(ManageMXBean.MANAGE_BEAN_NAME);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        mbs.registerMBean(new ManageMXBeanImpl(config), mxbeanName);
        
        registered = true;
    }

    public synchronized static void unregisterMXBean() throws Exception
    {
        if (!registered) {
            return;
        }
        
        ObjectName mxbeanName = new ObjectName(ManageMXBean.MANAGE_BEAN_NAME);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        mbs.unregisterMBean(mxbeanName);
        registered = false;
    }
}
