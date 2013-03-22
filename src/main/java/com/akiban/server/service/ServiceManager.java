/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
