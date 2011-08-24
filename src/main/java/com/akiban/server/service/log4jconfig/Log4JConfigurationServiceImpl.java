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

package com.akiban.server.service.log4jconfig;

import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;

public final class Log4JConfigurationServiceImpl
        implements Log4JConfigurationService, Service<Log4JConfigurationService>, JmxManageable {

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo(
                "Log4JConfig",
                Log4JConfigurationMXBeanSingleton.instance(),
                Log4JConfigurationMXBean.class
        );
    }

    @Override
    public Log4JConfigurationService cast() {
        return this;
    }

    @Override
    public Class<Log4JConfigurationService> castClass() {
        return Log4JConfigurationService.class;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
    
    @Override
    public void crash() {
    }
    
}
