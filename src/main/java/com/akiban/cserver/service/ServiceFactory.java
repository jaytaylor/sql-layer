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

package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.memcache.MemcacheService;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.session.SessionService;
import com.akiban.cserver.service.tree.TreeService;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;

public interface ServiceFactory
{
    Service<JmxRegistryService> jmxRegistryService();

    Service<ConfigurationService> configurationService();
    
    Service<SessionService> sessionService();

    Service<NetworkService> networkService();

    Service<CServer> chunkserverService();
    
    Service<TreeService> treeService();
    
    Service<SchemaManager> schemaManager();
    
    Service<Store> storeService();

    Service<MemcacheService> memcacheService();
}
