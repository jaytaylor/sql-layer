package com.akiban.cserver;

import com.akiban.cserver.store.Store;

public interface CustomQuery {

    public void setStore(final Store store);
    public void setParameters(Object[] parameters);
    public String getResult();
    public void runQuery() throws Exception;
    
}
