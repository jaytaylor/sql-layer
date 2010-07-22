package com.akiban.cserver.manage;

public interface ManageMXBean
{
    static final String MANAGE_BEAN_NAME = "com.akiban:type=Manage";
    
    void ping();
    
    void shutdown();
    
    int getNetworkPort();
    
    int getJmxPort();
    
    void enableVerboseLogging();
    
    void disableVerboseLogging();
    
    void startMessageCapture();
    
    void stopMessageCapture();
    
    void displayCapturedMessages();
    
    void clearCapturedMessages();
    
    void setMaxCapturedMessageCount(final int count);
    
    void enableExperimentalSchema();
    
    void disableExperimentalSchema();
    
    // TODO - temporary
    String copyBackPages();
    
}
