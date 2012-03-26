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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Column;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;

import java.util.*;

/** A SQL INSERT statement. */
public class InsertStatement extends BaseUpdateStatement
{
    private List<Column> targetColumns;

    public InsertStatement(PlanNode query, TableNode targetTable,
                           List<Column> targetColumns, EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query, targetTable, columnEquivalencies);
        this.targetColumns = targetColumns;
    }

    public List<Column> getTargetColumns() {
        return targetColumns;
    }

    @Override
    public String summaryString() {
        return super.summaryString() + "(" + getTargetTable() + targetColumns + ")";
    }
}
