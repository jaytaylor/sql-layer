
package com.akiban.util.tap;

public interface TapMXBean
{
    public void enableAll();

    public void disableAll();

    public void enableInitial();

    public void setEnabled(String regExPattern, boolean on);

    public void reset(String regExPattern);

    public void resetAll();

    public String getReport();

    public TapReport[] getReports(String regExPattern);
}
