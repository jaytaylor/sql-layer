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

public final class Log4JConfigurationMXBeanSingleton implements Log4JConfigurationMXBean {
    private final static Log4JConfigurationMXBean instance = new Log4JConfigurationMXBeanSingleton();

    public static Log4JConfigurationMXBean instance() {
        return instance;
    }

    private Log4JConfigurationMXBeanSingleton() {
        // nothing
    }

    @Override
    public String getConfigurationFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setConfigurationFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getUpdateFrequencyMS() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pollConfigurationFile(String file, long updateFrequencyMS) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pollConfigurationFile(long updateFrequencyMS) {
        throw new UnsupportedOperationException();
    }
}
