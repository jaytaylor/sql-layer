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

public final class StatisticsServiceImpl implements StatisticsService, Service<StatisticsService>, JmxManageable {
    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Statistics", new StatisticsServiceMXBeanImpl(), StatisticsServiceMXBean.class);
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
