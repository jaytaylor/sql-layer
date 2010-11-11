package com.akiban.cserver.service;

public interface ServiceManagerMXBean {
    boolean isStartupBlocked();
    void resumeStartup();
}
