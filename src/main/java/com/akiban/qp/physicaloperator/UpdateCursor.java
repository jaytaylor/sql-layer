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

public final class UpdateCursor implements Cursor {

    private final ModifiableCursor input;
    private final UpdateLambda updateLambda;

    private boolean currentRowIsMine = false;
    private Row currentRow;

    public UpdateCursor(ModifiableCursor input, UpdateLambda updateLambda) {
        this.input = input;
        this.updateLambda = updateLambda;
    }

    @Override
    public void open() {
        input.open();
    }

    @Override
    public boolean next() {
        if (input.next()) {
            Row input = this.input.currentRow();
            if (!updateLambda.rowIsApplicable(input)) {
                currentRowIsMine = false;
                return true;
            }
            currentRowIsMine = true;
            currentRow = updateLambda.applyUpdate(input);
            input.release();
            this.input.updateCurrentRow(currentRow);
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        input.close();
    }

    @Override
    public Row currentRow() {
        return currentRowIsMine ? currentRow : input.currentRow();
    }
}
