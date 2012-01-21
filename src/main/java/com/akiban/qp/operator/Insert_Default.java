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

/**

 <h1>Overview</h1>

 Inserts new rows into a table. This is an UpdatePlannable, not a PhysicalOperator.

 <h1>Arguments</h1>

 <ul>

 <li><b>PhysicalOperator inputOperator:</b> source of rows to be inserted

 </ul>

 <h1>Behaviour</h1>

 For each row in the insert operator, the row in inserted into the
 table. In practice, this is currently done via
 <i>StoreAdapater#insertRow</i>, which is implemented by
 <i>PersistitAdapater#insertRow</i>, which invokes
 <i>PersistitStore#insertRow</i>

 The result of this update is an UpdateResult instance which summarizes
 how many rows were updated and how long the operation took.

 <h1>Output</h1>

 N/A

 <h1>Assumptions</h1>

 The inputOperator is returning rows of the UserTableRowType of the table being inserted into.

 <h1>Performance</h1>

 Insert may be slow because because indexes are also updated. Insert
 may be able to be improved in performance by batching the index
 updates, but there is no current API to so.

 <h1>Memory Requirements</h1>

 None.

 */

class Insert_Default extends OperatorExecutionBase implements UpdatePlannable {

    public Insert_Default(Operator inputOperator) {
        this.inputOperator = inputOperator;
    }

    @Override
    public UpdateResult run(QueryContext context) {
        context(context);
        int seen = 0, modified = 0;
        INSERT_TAP.in();
        Cursor inputCursor = inputOperator.cursor(context);
        inputCursor.open();
        try {
            Row row;
            while ((row = inputCursor.next()) != null) {
                checkQueryCancelation();
                ++seen;
                adapter().writeRow(row);
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
