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

import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.util.RowDefNotFoundException;

class TopLevelWrappingCursor extends ChainedCursor {

    // Cursor interface

    @Override
    public void open(Bindings bindings) {
        try {
            super.open(bindings);
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public boolean next() {
        try {
            return super.next();
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public Row currentRow() {
        try {
            return super.currentRow();
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public void removeCurrentRow() {
        try {
            super.removeCurrentRow();
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public void updateCurrentRow(Row newRow) {
        try {
            super.updateCurrentRow(newRow);
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public ModifiableCursorBackingStore backingStore() {
        final ModifiableCursorBackingStore delegate = super.backingStore();
        return new ModifiableCursorBackingStore() {
            @Override
            public void addRow(RowBase newRow) {
                try {
                    delegate.addRow(newRow);
                } catch (RuntimeException e) {
                    throw launder(e);
                }
            }
        };
    }

    // WrappingCursor interface

    TopLevelWrappingCursor(Cursor input) {
        super(input);
    }

    // private methods

    private static RuntimeException launder(RuntimeException exception) {
        if (exception.getClass().equals(RowDefNotFoundException.class)) {
            RowDefNotFoundException casted = (RowDefNotFoundException) exception;
            throw new NoSuchTableException(casted.getId(), casted);
        }
        return exception;
    }
}
