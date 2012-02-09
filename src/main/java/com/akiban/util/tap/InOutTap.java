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
    
    public InOutTap createSubsidiaryTap(String name, InOutTap outermostRecursiveTap)
    {
        return internal.createSubsidiaryTap(name, outermostRecursiveTap);
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
