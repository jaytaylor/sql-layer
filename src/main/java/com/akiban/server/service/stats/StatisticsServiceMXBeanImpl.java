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

package com.akiban.server.service.stats;

import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.memcache.MemcacheService;

public final class StatisticsServiceMXBeanImpl implements StatisticsServiceMXBean {

    @Override
    public int getHapiRequestsCount() {
        return get(MemcacheService.class).getRequestsCount();
    }
    
    @Override
    public int getConnectionsOpened() {
        return get(MemcacheService.class).getConnectionsOpened();
    }

    @Override
    public int getConnectionsClosed() {
        return get(MemcacheService.class).getConnectionsClosed();
    }

    @Override
    public int getConnectionsErrored() {
        return get(MemcacheService.class).getConnectionsErrored();
    }

    @Override
    public int getConnectionsActive() {
        return get(MemcacheService.class).getConnectionsActive();
    }

    @Override
    public int getMysqlInsertsCount() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public int getMysqlDeletesCount() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public int getMysqlUpdatesCount() {
        throw new UnsupportedOperationException(); // TODO
    }

    private <T> T get(Class<T> serviceClass) {
        return ServiceManagerImpl.get().getServiceByClass(serviceClass);
    }
}
