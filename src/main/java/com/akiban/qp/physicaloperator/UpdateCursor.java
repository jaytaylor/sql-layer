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

import com.akiban.qp.row.ManagedRow;

final class UpdateCursor implements Cursor {

    private final ModifiableCursor inputs;
    private final UpdateLambda updateLambda;

    UpdateCursor(ModifiableCursor inputs, UpdateLambda updateLambda) {
        this.inputs = inputs;
        this.updateLambda = updateLambda;
    }

    @Override
    public void open() {
        inputs.open();
    }

    @Override
    public boolean next() {
        return inputs.next();
    }

    @Override
    public void close() {
        inputs.close();
    }

    @Override
    public ManagedRow currentRow() {
        ManagedRow input = inputs.currentRow();
        if (!updateLambda.rowIsApplicable(input)) {
            return input;
        }
        ManagedRow updated = updateLambda.applyUpdate(input);
        input.release();
        inputs.updateCurrentRow(updated);
        return updated;
    }
}
