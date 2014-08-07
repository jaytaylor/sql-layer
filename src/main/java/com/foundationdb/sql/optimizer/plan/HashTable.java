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

package com.foundationdb.sql.optimizer.plan;


/** A Bloom filter. */
public class HashTable extends BaseHashTable
{
    private long estimatedSize;
    private double selectivity;

    public HashTable(long estimatedSize, double selectivity) {
        this.estimatedSize = estimatedSize;
        this.selectivity = selectivity;
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }
    public double getSelectivity() {
        return selectivity;
    }

}