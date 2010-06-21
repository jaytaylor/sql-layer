package com.akiban.cserver.manage;

public interface ManageMXBean
{
    static final String MANAGE_BEAN_NAME = "com.akiban:type=Manage";
    
    void ping();
    
    void shutdown();
    
    int getNetworkPort();
    
    int getJmxPort();
}
