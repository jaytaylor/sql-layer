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

import java.util.*;

/**
 * An SQL SELECT statement turned into a simpler form for the interim
 * heuristic optimizer.
 * 
 * Takes care of representing what we can optimize today and rejecting
 * what we cannot.
 */
public class SimplifiedSelectQuery extends SimplifiedQuery
{
    // Turn the given SELECT statement into its simplified form.
    public SimplifiedSelectQuery(CursorNode cursor, Set<ValueNode> joinConditions)
            throws StandardException {
        super(cursor, joinConditions);

        if (cursor.getOrderByList() != null)
            fillFromOrderBy(cursor.getOrderByList());
        if (cursor.getOffsetClause() != null)
            fillOffset(cursor.getOffsetClause());
        if (cursor.getFetchFirstClause() != null)
            fillLimit(cursor.getFetchFirstClause());
        if (cursor.getUpdateMode() == CursorNode.UpdateMode.UPDATE)
            throw new UnsupportedSQLException("Unsupported FOR UPDATE");
    }

}
