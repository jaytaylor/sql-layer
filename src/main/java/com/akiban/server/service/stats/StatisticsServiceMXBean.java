
package com.akiban.server.service.stats;

import com.akiban.util.tap.TapReport;

@SuppressWarnings("unused") // jmx
public interface StatisticsServiceMXBean {

    public void enableAll();

    public void disableAll();

    public void setEnabled(final String regExPattern, final boolean on);

    public void reset(final String regExPattern);

    public void resetAll();

    public String getReport();

    public TapReport[] getReports(final String regExPattern);
}
