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

package com.foundationdb.server.store.statistics;

public class HistogramEntryDescription
{

    protected String keyString;
    protected long equalCount;
    protected long lessCount;
    protected long distinctCount;

    public HistogramEntryDescription(String keyString, long equalCount, long lessCount, long distinctCount) {
        this.distinctCount = distinctCount;
        this.equalCount = equalCount;
        this.keyString = keyString;
        this.lessCount = lessCount;
    }

    /** A user-visible form of the key for this entry. */
    public String getKeyString() {
        return keyString;
    }

    /** The number of samples that were equal to the key value. */
    public long getEqualCount() {
        return equalCount;
    }

    /** The number of samples that were less than the key value
     * (and greater than the previous entry's key value, if any).
     */
    public long getLessCount() {
        return lessCount;
    }

    /** The number of distinct values in the less-than range. */
    public long getDistinctCount() {
        return distinctCount;
    }

    @Override
    final public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HistogramEntryDescription)) return false;

        HistogramEntryDescription that = (HistogramEntryDescription) o;

        return distinctCount == that.distinctCount
                && equalCount == that.equalCount
                && lessCount == that.lessCount
                && keyString.equals(that.keyString);

    }

    @Override
    final public int hashCode() {
        int result = keyString.hashCode();
        result = 31 * result + (int) (equalCount ^ (equalCount >>> 32));
        result = 31 * result + (int) (lessCount ^ (lessCount >>> 32));
        result = 31 * result + (int) (distinctCount ^ (distinctCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" + getKeyString() +
                ": = " + getEqualCount() +
                ", < " + getLessCount() +
                ", distinct " + getDistinctCount() +
                "}";
    }
}
