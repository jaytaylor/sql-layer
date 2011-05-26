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

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.UserTable;

import java.util.*;

/**
 * An SQL table modifying statement turned into a simpler form for the
 * interim heuristic optimizer.
 */
public class SimplifiedTableStatement extends SimplifiedQuery
{
    private TableNode targetTable;

    public SimplifiedTableStatement(DMLModStatementNode statement, 
                                    Set<ValueNode> joinConditions)
            throws StandardException {
        super(statement, joinConditions);

        UserTable table = (UserTable)statement.getTargetTableName().getUserData();
        if (table == null)
            throw new StandardException("Table not bound properly.");
        targetTable = getTables().addNode(table, true);
    }

    public TableNode getTargetTable() {
        return targetTable;
    }

}
