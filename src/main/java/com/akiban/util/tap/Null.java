
package com.akiban.util.tap;

/**
 * A {@link com.akiban.util.tap.Tap} subclass that does nothing. This is the initial target of
 * every added {@link com.akiban.util.tap.Dispatch}.
 */
class Null extends Tap
{

    public Null(final String name)
    {
        super(name);
    }

    public void in()
    {
        // do nothing
    }

    public void out()
    {
        // do nothing
    }

    public long getDuration()
    {
        return 0;
    }

    public void reset()
    {
        // do nothing
    }

    public void appendReport(String label, final StringBuilder buffer)
    {
        // do nothing;
    }

    public String toString()
    {
        return "NullTap(" + name + ")";
    }

    public TapReport[] getReports()
    {
        return null;
    }
}
