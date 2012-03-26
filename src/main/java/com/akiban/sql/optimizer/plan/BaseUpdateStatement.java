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

import com.akiban.sql.optimizer.rule.EquivalenceFinder;

/** A statement that modifies the database and returns row counts.
 */
public class BaseUpdateStatement extends BaseStatement
{
    private TableNode targetTable;

    protected BaseUpdateStatement(PlanNode query, TableNode targetTable,
                                  EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query, columnEquivalencies);
        this.targetTable = targetTable;
    }

    public TableNode getTargetTable() {
        return targetTable;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        targetTable = map.duplicate(targetTable);
    }

}
