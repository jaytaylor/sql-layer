/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

package com.foundationdb.server.spatial;

import com.geophile.z.Record;
import com.geophile.z.SpatialObject;
import com.geophile.z.index.RecordWithSpatialObject;

import java.util.Comparator;

public class TestRecord extends RecordWithSpatialObject
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("(%s: %s)", soid, super.toString());
    }

    @Override
    public int hashCode()
    {
        return super.hashCode() ^ soid;
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean eq = false;
        if (super.equals(obj) && obj instanceof TestRecord) {
            TestRecord that = (TestRecord) obj;
            eq = this.soid == that.soid;
        }
        return eq;
    }

    // Record interface

    @Override
    public void copyTo(Record record)
    {
        super.copyTo(record);
        ((TestRecord)record).soid = this.soid;
    }

    // TestRecord interface

    public int soid()
    {
        return soid;
    }

    public void soid(int newId)
    {
        soid = newId;
    }

    public TestRecord(SpatialObject spatialObject)
    {
        spatialObject(spatialObject);
        soid = 0;
    }

    public TestRecord(SpatialObject spatialObject, int soid)
    {
        spatialObject(spatialObject);
        this.soid = soid;
    }

    public TestRecord()
    {}

    // Class state

    public static final Comparator<TestRecord> COMPARATOR =
        new Comparator<TestRecord>()
        {
            @Override
            public int compare(TestRecord r, TestRecord s)
            {
                return
                    r.z() < s.z()
                    ? -1
                    : r.z() > s.z()
                      ? 1
                      : r.soid < s.soid
                        ? -1
                        : r.soid > s.soid
                          ? 1
                          : 0;
            }
        };

    private static final int UNDEFINED_SOID = -1;

    // Object state

    private int soid = UNDEFINED_SOID;

    // Inner classes

    public static class Factory implements Record.Factory<TestRecord>
    {
        @Override
        public TestRecord newRecord()
        {
            return new TestRecord(spatialObject, id);
        }

        public Factory setup(SpatialObject spatialObject, int id)
        {
            this.spatialObject = spatialObject;
            this.id = id;
            return this;
        }

        private SpatialObject spatialObject;
        private int id;
    }
}
