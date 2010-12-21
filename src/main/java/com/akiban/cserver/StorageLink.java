package com.akiban.cserver;

public interface StorageLink {
    String getSchemaName();
    String getTableName();
    String getIndexName();
    String getTreeName();
    void setStorageCache(Object object);
    Object getStorageCache();
}
