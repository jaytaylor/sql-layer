package com.akiban.cserver;

import com.akiban.cserver.service.ServiceManager;

public interface CustomQuery {

    public void setServiceManager(final ServiceManager serviceManager);
    public void setParameters(Object[] parameters);
    public String getResult();
    public void runQuery() throws Exception;
    
}
