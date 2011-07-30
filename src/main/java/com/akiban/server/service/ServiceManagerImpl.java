/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service;

import java.util.concurrent.atomic.AtomicReference;

import com.akiban.server.AkServer;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.instrumentation.InstrumentationService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.memcache.MemcacheService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.sql.pg.PostgresService;

public final class ServiceManagerImpl implements ServiceManager
{
    private static final AtomicReference<ServiceManager> instance = new AtomicReference<ServiceManager>(null);

    public static void setServiceManager(ServiceManager newInstance) {
        if (newInstance == null) {
            instance.set(null);
        } else if (!instance.compareAndSet(null, newInstance)) {
            throw new RuntimeException(
                    "Tried to install a ServiceManager, but one was already set");
        }
    }

    private ServiceManagerImpl() {}

    /**
     * Gets the active ServiceManager; you can then use the returned instance to get any service you want.
     * @return the active ServiceManager
     * @deprecated for new code, please just use dependency injection
     */
    @Deprecated
    public static ServiceManager get() {
        return installed();
    }

    /**
     * Convenience for {@code ServiceManagerImpl.get().getSessionService().createSession()}
     * @return a new Session
     */
    public static Session newSession() {
        return installed().getSessionService().createSession();
    }

    // ServiceManager interface

    @Override
    public void startServices() throws ServiceStartupException {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public void stopServices() throws Exception {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public void crashServices() throws Exception {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public ConfigurationService getConfigurationService() {
        return installed().getConfigurationService();
    }

    @Override
    public AkServer getAkSserver() {
        return installed().getAkSserver();
    }

    @Override
    public Store getStore() {
        return installed().getStore();
    }

    @Override
    public TreeService getTreeService() {
        return installed().getTreeService();
    }

    @Override
    public MemcacheService getMemcacheService() {
        return installed().getMemcacheService();
    }

    @Override
    public PostgresService getPostgresService() {
        return installed().getPostgresService();
    }

    @Override
    public SchemaManager getSchemaManager() {
        return installed().getSchemaManager();
    }

    @Override
    public JmxRegistryService getJmxRegistryService() {
        return installed().getJmxRegistryService();
    }

    @Override
    public StatisticsService getStatisticsService() {
        return installed().getStatisticsService();
    }

    @Override
    public SessionService getSessionService() {
        return installed().getSessionService();
    }

    @Override
    public <T> T getServiceByClass(Class<T> serviceClass) {
        return installed().getServiceByClass(serviceClass);
    }

    @Override
    public DXLService getDXL() {
        return installed().getDXL();
    }

    @Override
    public boolean serviceIsStarted(Class<?> serviceClass) {
        return installed().serviceIsStarted(serviceClass);
    }

    @Override
    public InstrumentationService getInstrumentationService() {
        return installed().getInstrumentationService();
    }

    private static ServiceManager installed() {
        ServiceManager sm = instance.get();
        if (sm == null) {
            throw new ServiceNotStartedException("services haven't been started");
        }
        return sm;
    }
}
