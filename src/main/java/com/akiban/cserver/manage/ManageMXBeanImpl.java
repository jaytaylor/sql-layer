package com.akiban.cserver.manage;

import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerConfig;

class ManageMXBeanImpl implements ManageMXBean
{
    private CServerConfig config;
    
    protected ManageMXBeanImpl(CServerConfig config)
    {
        this.config = config;
    }
    
    @Override
    public void ping()
    {
        return;
    }

    @Override
    public void shutdown()
    {
        new Thread()
        {
            @Override public void run()
            {
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
                System.exit(0);
            }
        }.start();
    }

    @Override
    public int getNetworkPort()
    {
        String value = config.property(CServer.P_CSERVER_PORT);
        return Integer.parseInt(value);
    }
    
    @Override
    public int getJmxPort()
    {
        int jmxPort = Integer.getInteger("com.sun.management.jmxremote.port", 0);
        return jmxPort;
    }
}
