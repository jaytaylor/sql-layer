package com.akiban.cserver.service.memcache;

public interface MemcacheMXBean {
    public String getOutputFormat();
    public void setOutputFormat(String whichFormat);
    public String[] getAvailableOutputFormats();
}
