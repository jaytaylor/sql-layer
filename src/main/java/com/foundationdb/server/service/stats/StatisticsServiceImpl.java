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

package com.foundationdb.server.service.stats;

import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import com.google.inject.Inject;

public final class StatisticsServiceImpl implements StatisticsService,
        Service, JmxManageable {

    private static final String STATISTICS_PROPERTY = "fdbsql.statistics";

    @Inject
    public StatisticsServiceImpl(ConfigurationService config) {
        this.config = config;
    }

    @Override
    public void setEnabled(final String regExPattern, final boolean on) {
        Tap.setEnabled(regExPattern, on);
    }

    @Override
    public void reset(final String regExPattern) {
        Tap.reset(regExPattern);
    }

    @Override
    public TapReport[] getReport(final String regExPattern) {
        return Tap.getReport(regExPattern);
    }

    private final StatisticsServiceMXBean bean = new StatisticsServiceMXBean() {

        @Override
        public void disableAll() {
            Tap.setEnabled(".*", false);
        }

        @Override
        public void enableAll() {
            Tap.setEnabled(".*", true);
        }

        @Override
        public String getReport() {
            return Tap.report();
        }

        @Override
        public TapReport[] getReports(String regExPattern) {
            return Tap.getReport(regExPattern);
        }

        @Override
        public void reset(String regExPattern) {
            Tap.reset(regExPattern);
        }

        @Override
        public void resetAll() {
            Tap.reset(".*");
        }

        @Override
        public void setEnabled(String regExPattern, boolean on) {
            Tap.setEnabled(regExPattern, on);
        }
    };

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Statistics", bean,
                StatisticsServiceMXBean.class);
    }

    @Override
    public void start() {
        String stats_enable = config.getProperty(STATISTICS_PROPERTY);

        if (stats_enable.length() > 0) {
            Tap.setEnabled(stats_enable, true);
        }
    }

    @Override
    public void stop() {
    }
    
    
    @Override
    public void crash() {
    }

    private final ConfigurationService config;
}
