
package com.akiban.server.service.stats;

import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import com.google.inject.Inject;

public final class StatisticsServiceImpl implements StatisticsService,
        Service, JmxManageable {

    private static final String STATISTICS_PROPERTY = "akserver.statistics";

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
