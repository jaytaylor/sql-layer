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

package com.foundationdb.qp.operator;

import com.foundationdb.qp.exec.UpdateResult;


public final class StandardUpdateResult implements UpdateResult {

    // CudResult interface
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
                "CudResult(touched: %d, modified: %d)",
                touched,
                modified
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StandardUpdateResult that = (StandardUpdateResult) o;

        return modified == that.modified && touched == that.touched;

    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + touched;
        result = 31 * result + modified;
        return result;
    }

    // StandardCudResult interface

    public StandardUpdateResult(int rowsTouched, int rowsModified) {
        this.touched = rowsTouched;
        this.modified = rowsModified;
    }

    // state
    private final int touched;
    private final int modified;
}
