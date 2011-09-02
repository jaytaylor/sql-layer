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

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.unparser.NodeToString;

/** A parsed (and type-bound, normalized, etc.) SQL query.
 */
public class AST extends BasePlanNode
{
    private DMLStatementNode statement;

    public AST(DMLStatementNode statement) {
        this.statement = statement;
    }
    
    public DMLStatementNode getStatement() {
        return statement;
    }

    @Override
    public String toString() {
        try {
            return new NodeToString().toString(statement);
        }
        catch (StandardException ex) {
            return super.toString();
        }
    }

}
