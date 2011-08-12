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

import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.sql.parser.*;

import com.akiban.ais.model.Column;

import com.akiban.sql.StandardException;

import java.util.*;

/**
 * An SQL SELECT statement turned into a simpler form for the interim
 * heuristic optimizer.
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

    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        str.append("\ngroup: ");
        str.append(getGroup());
        if (getJoins() != null) {
            str.append("\njoins: ");
            str.append(getJoins());
        }
        else if (getValues() != null) {
            str.append("\nvalues: ");
            str.append(getValues());
        }
        if (getSelectColumns() != null) {
            str.append("\nselect: [");
            for (int i = 0; i < getSelectColumns().size(); i++) {
                if (i > 0) str.append(", ");
                str.append(getSelectColumns().get(i));
            }
            str.append("]");
        }
        if (!getConditions().isEmpty()) {
            str.append("\nconditions: ");
            for (int i = 0; i < getConditions().size(); i++) {
                if (i > 0) str.append(",\n  ");
                str.append(getConditions().get(i));
            }
        }
        if (getSortColumns() != null) {
            str.append("\nsort: ");
            for (int i = 0; i < getSortColumns().size(); i++) {
                if (i > 0) str.append(", ");
                str.append(getSortColumns().get(i));
            }
        }
        if (getOffset() > 0) {
            str.append("\noffset: ");
            str.append(getOffset());
        }
        if (getLimit() >= 0) {
            str.append("\nlimit: ");
            str.append(getLimit());
        }
        str.append("\nequivalences: ");
        for (int i = 0; i < getColumnEquivalences().size(); i++) {
            if (i > 0) str.append(",\n  ");
            int j = 0;
            for (Column column : getColumnEquivalences().get(i)) {
                if (j++ > 0) str.append(" = ");
                str.append(column);
            }
        }
        return str.toString();
    }

}
