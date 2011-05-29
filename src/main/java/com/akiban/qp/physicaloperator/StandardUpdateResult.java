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

package com.akiban.qp.physicaloperator;

import com.akiban.qp.exec.UpdateResult;


public final class StandardUpdateResult implements UpdateResult {

    // CudResult interface
    @Override
    public long executionTimeInMS() {
        return time;
    }

    @Override
    public int rowsTouched() {
        return touched;
    }

    @Override
    public int rowsModified() {
        return modified;
    }

    // Object interface


    @Override
    public String toString() {
        return String.format(
                "CudResult(time: %dms (%.2fs), touched: %d, modified: %d)",
                time,
                ((double)time)/1000L,
                touched,
                modified
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StandardUpdateResult that = (StandardUpdateResult) o;

        return modified == that.modified && time == that.time && touched == that.touched;

    }

    @Override
    public int hashCode() {
        int result = (int) (time ^ (time >>> 32));
        result = 31 * result + touched;
        result = 31 * result + modified;
        return result;
    }

    // StandardCudResult interface

    public StandardUpdateResult(long timeTookMS, int rowsTouched, int rowsModified) {
        this.time = timeTookMS;
        this.touched = rowsTouched;
        this.modified = rowsModified;
    }

    // state

    private final long time;
    private final int touched;
    private final int modified;
}
