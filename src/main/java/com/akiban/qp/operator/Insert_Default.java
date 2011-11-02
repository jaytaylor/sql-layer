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
package com.akiban.qp.operator;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.row.Row;
import com.akiban.util.Strings;
import com.akiban.util.Tap;

class Insert_Default extends OperatorExecutionBase implements UpdatePlannable {

    public Insert_Default(Operator inputOperator) {
        this.inputOperator = inputOperator;
    }

    @Override
    public UpdateResult run(Bindings bindings, StoreAdapter adapter) {
        adapter(adapter);
        int seen = 0, modified = 0;
        INSERT_TAP.in();
        Cursor inputCursor = inputOperator.cursor(adapter);
        inputCursor.open(bindings);
        try {
            Row row;
            while ((row = inputCursor.next()) != null) {
                checkQueryCancelation();
                ++seen;
                adapter.writeRow(row, bindings);
                ++modified;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Insert: row {}", row);
                }

            }
        } finally {
            inputCursor.close();
            INSERT_TAP.out();
        }
        return new StandardUpdateResult(seen, modified);
    }

    @Override
    public String describePlan() {
        return describePlan(inputOperator);
    }

    @Override
    public String describePlan(Operator inputOperator) {
        return inputOperator + Strings.nl() + this;
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), inputOperator);
    }

    private final Operator inputOperator;
    private static final Tap.InOutTap INSERT_TAP = Tap.createTimer("operator: insert");
    private static final Logger LOG = LoggerFactory.getLogger(Insert_Default.class);

}
