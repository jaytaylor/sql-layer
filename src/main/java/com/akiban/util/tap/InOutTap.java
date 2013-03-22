
package com.akiban.util.tap;

public class InOutTap
{
    // InOutTap interface

    public void in()
    {
        internal.in();
    }

    public void out()
    {
        internal.out();
    }

    /**
     * Reset the tap.
     */
    public void reset()
    {
        internal.reset();
    }
    
    public InOutTap createSubsidiaryTap(String name)
    {
        return internal.createSubsidiaryTap(name, this);
    }

    /**
     * Gets the duration of the tap's in-to-out timer.
     *
     * @return the tap's duration
     */
    public long getDuration()
    {
        return internal.getDuration();
    }

    /**
     * Gets the tap's report
     *
     * @return the underlying tap's report
     */
    public TapReport[] getReports()
    {
        return internal.getReports();
    }

    // For use by this package
    
    Tap internal()
    {
        return internal;
    }

    InOutTap(Tap internal)
    {
        this.internal = internal;
    }

    // Object state

    private final Tap internal;
}
