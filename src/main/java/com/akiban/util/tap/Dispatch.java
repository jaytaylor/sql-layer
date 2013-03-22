
package com.akiban.util.tap;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link com.akiban.util.tap.Tap} Implementation that simply dispatches to another Tap
 * instance. Used to hold the "switch" that determines whether a Null,
 * TimeAndCount or other kind of Tap is invoked.
 * <p/>
 * Reason for this dispatch mechanism is that HotSpot seems to be able to
 * optimize out a dispatch to an instance of Null. Therefore code can invoke
 * the methods of a Dispatch object set to Null with low or no performance
 * penalty.
 */
class Dispatch extends Tap
{
    // Tap interface

    public void in()
    {
        currentTap.in();
    }

    public void out()
    {
        currentTap.out();
    }
    
    public long getDuration()
    {
        return currentTap.getDuration();
    }

    public void reset()
    {
        currentTap.reset();
    }

    public void appendReport(String label, StringBuilder buffer)
    {
        currentTap.appendReport(label, buffer);
    }

    public TapReport[] getReports()
    {
        return currentTap.getReports();
    }

    public String toString()
    {
        return currentTap.toString();
    }
    
    // Dispatch interface
    
    public Tap enabledTap()
    {
        return enabledTap;
    }

    public void setEnabled(boolean on)
    {
        // If a tap is enabled between in and out calls, then the nesting will appear to be off.
        // markEnabled causes the nesting check to be skipped the first time out is called after
        // enabling.
        if (on) {
            enabledTap.reset();
            currentTap = enabledTap;
        } else {
            enabledTap.disable();
            currentTap = new Null(name);
        }
        for (Dispatch subsidiaryDispatch : subsidiaryDispatches) {
            subsidiaryDispatch.setEnabled(on);
        }
    }

    public void addSubsidiaryDispatch(Dispatch subsidiaryDispatch)
    {
        subsidiaryDispatches.add(subsidiaryDispatch);
    }

    public boolean isSubsidiary()
    {
        return enabledTap.isSubsidiary();
    }

    public Dispatch(String name, Tap tap)
    {
        super(name);
        this.currentTap = new Null(name);
        this.enabledTap = tap;
    }

    // Object state

    private Tap currentTap;
    private Tap enabledTap;
    // For recursive taps
    private List<Dispatch> subsidiaryDispatches = new ArrayList<>();
}
