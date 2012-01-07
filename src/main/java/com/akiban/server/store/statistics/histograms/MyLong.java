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

package com.akiban.server.store.statistics.histograms;

public final class MyLong {

//    TODO REMOVE THIS CLASS
//    THIS IS ONLY HERE TO AS A SEMANTICLY STRICTER STANDIN FOR LONG


    @Override
    public String toString() {
        return Long.toString(val());
    }

    public long val() {
        return value;
    }

    public MyLong(long value) {
        this.value = value;
    }

    private final long value;
}
