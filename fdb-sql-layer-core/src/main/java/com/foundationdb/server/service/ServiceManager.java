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

package com.foundationdb.server.service;

import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.server.error.ServiceAlreadyStartedException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.monitor.MonitorService;
import com.foundationdb.server.service.jmx.JmxRegistryService;
import com.foundationdb.server.service.servicemanager.ServiceManagerBase;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.stats.StatisticsService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ServiceManager extends ServiceManagerBase {
    static final Logger logger = LoggerFactory.getLogger(ServiceManager.class);

    enum State { IDLE, STARTING, ACTIVE, STOPPING, ERROR_STARTING };

    State getState();

    void startServices() throws ServiceAlreadyStartedException;

    void stopServices() throws Exception;
    
    void crashServices() throws Exception;

    ConfigurationService getConfigurationService();
    
    LayerInfoInterface getLayerInfo();

    Store getStore();

    SchemaManager getSchemaManager();

    JmxRegistryService getJmxRegistryService();
    
    StatisticsService getStatisticsService();

    SessionService getSessionService();

    <T> T getServiceByClass(Class<T> serviceClass);

    DXLService getDXL();

    boolean serviceIsStarted(Class<?> serviceClass);
    
    MonitorService getMonitorService();

    boolean serviceIsBoundTo(Class<?> serviceClass, Class<?> implClass);
}
