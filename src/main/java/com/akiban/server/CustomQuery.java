
package com.akiban.server;

import com.akiban.server.service.ServiceManager;

public interface CustomQuery {

    public void setServiceManager(final ServiceManager serviceManager);
    public void setParameters(Object[] parameters);
    public String getResult();
    public void runQuery() throws Exception;
    public void stopQuery() throws Exception;
    
}
