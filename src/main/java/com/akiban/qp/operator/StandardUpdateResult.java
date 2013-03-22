
package com.akiban.qp.operator;

import com.akiban.qp.exec.UpdateResult;


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
