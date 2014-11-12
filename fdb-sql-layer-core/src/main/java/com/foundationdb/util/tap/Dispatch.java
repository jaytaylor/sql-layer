/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.util.tap;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link com.foundationdb.util.tap.Tap} Implementation that simply dispatches to another Tap
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
