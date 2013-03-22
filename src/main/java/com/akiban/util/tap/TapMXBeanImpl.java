
package com.akiban.util.tap;

public class TapMXBeanImpl implements TapMXBean
{
    // TapMXBean interface

    @Override
    public String getReport()
    {
        return Tap.report();
    }

    @Override
    public void reset(String regExPattern)
    {
        Tap.reset(regExPattern);
    }

    @Override
    public void resetAll()
    {
        Tap.reset(".*");
    }

    @Override
    public void disableAll()
    {
        Tap.setEnabled(".*", false);
    }

    @Override
    public void enableAll()
    {
        Tap.setEnabled(".*", true);
    }

    @Override
    public void enableInitial()
    {
        Tap.enableInitial();
    }

    @Override
    public void setEnabled(final String regExPattern, final boolean on)
    {
        Tap.setEnabled(regExPattern, on);
    }

    @Override
    public TapReport[] getReports(final String regExPattern)
    {
        return Tap.getReport(regExPattern);
    }
}
