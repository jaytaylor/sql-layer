/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.util.tap;

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
    
    public void markEnabled()
    {
        currentTap.markEnabled();
    }
    
    public long getDuration()
    {
        return currentTap.getDuration();
    }

    public void reset()
    {
        currentTap.reset();
    }

    public void appendReport(final StringBuilder sb)
    {
        currentTap.appendReport(sb);
    }

    public TapReport getReport()
    {
        return currentTap.getReport();
    }

    public String toString()
    {
        return currentTap.toString();
    }
    
    // Dispatch interface

    public Dispatch(String name, Tap tap)
    {
        super(name);
        this.currentTap = new Null(name);
        this.enabledTap = tap;
    }

    public void setEnabled(boolean on)
    {
        // If a tap is enabled between in and out calls, then the nesting will appear to be off.
        // markEnabled causes the nesting check to be skipped the first time out is called after
        // enabling.
        enabledTap.markEnabled();
        currentTap = on ? enabledTap : new Null(name);
    }
    
    // For use by this package
    
    void setEnabledTap(Tap tap)
    {
        enabledTap = tap;
    }
    
    // Object state

    private Tap currentTap;
    private Tap enabledTap;
}
