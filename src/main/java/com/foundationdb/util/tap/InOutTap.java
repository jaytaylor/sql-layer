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
