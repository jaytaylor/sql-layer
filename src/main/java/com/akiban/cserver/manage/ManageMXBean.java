package com.akiban.cserver.manage;

public interface ManageMXBean
{
    static final String MANAGE_BEAN_NAME = "com.akiban:type=Manage";
    
    void ping();
    
    void shutdown();
    
    int getNetworkPort();
    
    int getJmxPort();
    
    boolean isVerboseLoggingEnabled();
    
    void enableVerboseLogging();
    
    void disableVerboseLogging();
    
    void startMessageCapture();
    
    void stopMessageCapture();
    
    void displayCapturedMessages();
    
    void clearCapturedMessages();
    
    void setMaxCapturedMessageCount(final int count);
    
    boolean isDeferIndexesEnabled();
    
    void setDeferIndexes(final boolean defer);
    
    void buildIndexes(final String arg);
    
    void deleteIndexes(final String arg);
    
    void flushIndexes();
    
    // TODO - temporary
    String copyBackPages();
    
}
