
package com.akiban.server.service.servicemanager;

import com.akiban.server.AkServerInterface;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.monitor.MonitorService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

public abstract class DelegatingServiceManager implements ServiceManager {

    // ServiceManager interface

    @Override
    public State getState() {
        return delegate().getState();
    }

    @Override
    public void startServices() throws ServiceStartupException {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public void stopServices() {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public void crashServices() {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public ConfigurationService getConfigurationService() {
        return delegate().getConfigurationService();
    }

    @Override
    public AkServerInterface getAkSserver() {
        return delegate().getAkSserver();
    }

    @Override
    public Store getStore() {
        return delegate().getStore();
    }

    @Override
    public TreeService getTreeService() {
        return delegate().getTreeService();
    }

    @Override
    public SchemaManager getSchemaManager() {
        return delegate().getSchemaManager();
    }

    @Override
    public JmxRegistryService getJmxRegistryService() {
        return delegate().getJmxRegistryService();
    }

    @Override
    public StatisticsService getStatisticsService() {
        return delegate().getStatisticsService();
    }

    @Override
    public SessionService getSessionService() {
        return delegate().getSessionService();
    }

    @Override
    public <T> T getServiceByClass(Class<T> serviceClass) {
        return delegate().getServiceByClass(serviceClass);
    }

    @Override
    public DXLService getDXL() {
        return delegate().getDXL();
    }

    @Override
    public boolean serviceIsStarted(Class<?> serviceClass) {
        return delegate().serviceIsStarted(serviceClass);
    }

    @Override
    public MonitorService getMonitorService() {
        return delegate().getMonitorService();
    }

    protected abstract ServiceManager delegate();
}
