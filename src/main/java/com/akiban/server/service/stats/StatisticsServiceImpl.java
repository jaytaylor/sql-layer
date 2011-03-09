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

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class StatisticsServiceImpl implements StatisticsService, Service<StatisticsService>, JmxManageable {

    private Map<CountingStat,AtomicInteger> countingStats;

    public StatisticsServiceImpl() {
        final EnumMap<CountingStat,AtomicInteger> tmp = new EnumMap<CountingStat, AtomicInteger>(CountingStat.class);
        for (CountingStat stat : CountingStat.values()) {
            tmp.put(stat, new AtomicInteger(0));
        }
        countingStats = Collections.unmodifiableMap(tmp);
    }

    private final StatisticsServiceMXBean bean = new StatisticsServiceMXBean() {

        private int get(CountingStat which) {
            return countingStats.get(which).get();
        }

        @Override
        public int getHapiRequestsCount() {
            return get(CountingStat.HAPI_REQUESTS);
        }

        @Override
        public int getConnectionsOpened() {
            return get(CountingStat.CONNECTIONS_OPENED);
        }

        @Override
        public int getConnectionsClosed() {
            return get(CountingStat.CONNECTIONS_CLOSED);
        }

        @Override
        public int getConnectionsErrored() {
            return get(CountingStat.CONNECTIONS_ERRORED);
        }

        @Override
        public int getMysqlInsertsCount() {
            return get(CountingStat.INSERTS);
        }

        @Override
        public int getMysqlDeletesCount() {
            return get(CountingStat.DELETES);
        }

        @Override
        public int getMysqlUpdatesCount() {
            return get(CountingStat.UPDATES);
        }
    };

    @Override
    public void incrementCount(CountingStat stat) {
        countingStats.get(stat).incrementAndGet();
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Statistics", bean, StatisticsServiceMXBean.class);
    }

    @Override
    public StatisticsService cast() {
        return this;
    }

    @Override
    public Class<StatisticsService> castClass() {
        return StatisticsService.class;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }
}
