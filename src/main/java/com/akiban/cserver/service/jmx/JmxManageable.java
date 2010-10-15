package com.akiban.cserver.service.jmx;

public interface JmxManageable {
    public String getJmxObjectName();
    public Object getJmxObject();
}
