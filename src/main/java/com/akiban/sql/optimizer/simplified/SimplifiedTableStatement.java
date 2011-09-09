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

package com.akiban.sql.optimizer.simplified;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.sql.parser.DMLModStatementNode;
import com.akiban.sql.parser.ValueNode;

import java.util.*;

/**
 * An SQL table modifying statement turned into a simpler form for the
 * interim heuristic optimizer.
 */
public abstract class SimplifiedTableStatement extends SimplifiedQuery
{
    private TableNode targetTable;

    public SimplifiedTableStatement(DMLModStatementNode statement, 
                                    Set<ValueNode> joinConditions) {
        super(statement, joinConditions);

        UserTable table = (UserTable)statement.getTargetTableName().getUserData();
        if (table == null)
            throw new NoSuchTableException (statement.getTargetTableName().getSchemaName(), statement.getTargetTableName().getTableName());
        targetTable = getTables().addNode(table);
    }

    public TableNode getTargetTable() {
        return targetTable;
    }

    public void recomputeUsed() {
        super.recomputeUsed();
        targetTable.setUsed(true);
    }
    
    public abstract List<TargetColumn> getTargetColumns();
    public abstract ColumnExpressionToIndex getFieldOffset();


    public static class TargetColumn {
        private Column column;
        private SimpleExpression value;

        public TargetColumn(Column column, SimpleExpression value) {
            this.column = column;
            this.value = value;
        }

        public Column getColumn() {
            return column;
        }
        public SimpleExpression getValue() {
            return value;
        }
    }
}
