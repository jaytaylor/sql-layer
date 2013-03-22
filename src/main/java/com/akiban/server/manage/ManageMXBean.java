
package com.akiban.server.manage;

@SuppressWarnings("unused") // used via JMX
public interface ManageMXBean {
    static final String MANAGE_BEAN_NAME = "com.akiban:type=Manage";

    void ping();

    int getJmxPort();

    boolean isDeferIndexesEnabled();

    void setDeferIndexes(final boolean defer);

    void buildIndexes(final String arg, final boolean deferIndexes);

    void deleteIndexes(final String arg);

    void flushIndexes();

    String getVersionString();
}
