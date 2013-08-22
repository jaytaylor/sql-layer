/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.servicemanager;

import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.server.error.ServiceStartupException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.jmx.JmxRegistryService;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.stats.StatisticsService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;

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
    public LayerInfoInterface getLayerInfo() {
        return delegate().getLayerInfo();
    }

    @Override
    public Store getStore() {
        return delegate().getStore();
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
    public boolean serviceIsBoundTo(Class<?> serviceClass, Class<?> implClass) {
        return delegate().serviceIsBoundTo(serviceClass, implClass);
    }

    @Override
    public MonitorService getMonitorService() {
        return delegate().getMonitorService();
    }

    protected abstract ServiceManager delegate();
}
