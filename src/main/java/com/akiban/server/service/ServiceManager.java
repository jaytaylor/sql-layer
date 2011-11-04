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

import com.akiban.server.AkServer;
import com.akiban.server.AkServerInterface;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.instrumentation.InstrumentationService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.memcache.MemcacheService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.sql.pg.PostgresService;

public interface ServiceManager {

    void startServices() throws ServiceStartupException;

    void stopServices() throws Exception;
    
    void crashServices() throws Exception;

    ConfigurationService getConfigurationService();
    
    AkServerInterface getAkSserver();

    Store getStore();
    
    TreeService getTreeService();

    MemcacheService getMemcacheService();

    PostgresService getPostgresService();

    SchemaManager getSchemaManager();

    JmxRegistryService getJmxRegistryService();
    
    StatisticsService getStatisticsService();

    SessionService getSessionService();

    <T> T getServiceByClass(Class<T> serviceClass);

    DXLService getDXL();

    boolean serviceIsStarted(Class<?> serviceClass);
    
    InstrumentationService getInstrumentationService();
}
