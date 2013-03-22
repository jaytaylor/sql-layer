
package com.akiban.server.service;

import com.akiban.server.AkServerInterface;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.monitor.MonitorService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.servicemanager.ServiceManagerBase;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ServiceManager extends ServiceManagerBase {
    static final Logger logger = LoggerFactory.getLogger(ServiceManager.class);

    enum State { IDLE, STARTING, ACTIVE, STOPPING, ERROR_STARTING };

    State getState();

    void startServices() throws ServiceStartupException;

    void stopServices() throws Exception;
    
    void crashServices() throws Exception;

    ConfigurationService getConfigurationService();
    
    AkServerInterface getAkSserver();

    Store getStore();

    TreeService getTreeService();

    SchemaManager getSchemaManager();

    JmxRegistryService getJmxRegistryService();
    
    StatisticsService getStatisticsService();

    SessionService getSessionService();

    <T> T getServiceByClass(Class<T> serviceClass);

    DXLService getDXL();

    boolean serviceIsStarted(Class<?> serviceClass);
    
    MonitorService getMonitorService();
}
