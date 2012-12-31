/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
