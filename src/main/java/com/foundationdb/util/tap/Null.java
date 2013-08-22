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

/**
 * A {@link com.foundationdb.util.tap.Tap} subclass that does nothing. This is the initial target of
 * every added {@link com.foundationdb.util.tap.Dispatch}.
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
