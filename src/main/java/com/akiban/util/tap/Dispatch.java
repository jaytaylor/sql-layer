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
    private List<Dispatch> subsidiaryDispatches = new ArrayList<Dispatch>();
}
