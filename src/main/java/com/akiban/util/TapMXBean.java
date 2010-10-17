package com.akiban.util;

public interface TapMXBean {

    public void enableAll();

    public void disableAll();

    public void setEnabled(final String regExPattern, final boolean on);

    public void setCustomTap(final String regExPattern, final String className)
            throws Exception;

    public void reset(final String regExPattern);

    public void resetAll();

    public String getReport();

    public TapReport[] getReports(final String regExPattern);

}
