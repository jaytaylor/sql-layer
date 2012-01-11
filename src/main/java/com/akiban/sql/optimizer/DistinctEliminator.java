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
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;

import java.util.*;

/** Eliminate DISTINCT from SELECT when result is already distinct.
 *
 * Derby has a somewhat different version of this.
 *
 * It would be nicer if this could be an actual rule, but it really
 * has to run before ASTStatementLoader to keep from trying to sort on
 * all the extra columns.
 */
public class DistinctEliminator
{
    SQLParserContext parserContext;
    NodeFactory nodeFactory;
    public DistinctEliminator(SQLParserContext parserContext) {
        this.parserContext = parserContext;
        this.nodeFactory = parserContext.getNodeFactory();
    }

    public DMLStatementNode eliminate(DMLStatementNode stmt) throws StandardException {
        ResultSetNode resultSet = stmt.getResultSetNode();
        if (resultSet.getNodeType() == NodeTypes.SELECT_NODE) {
            selectNode((SelectNode)resultSet);
        }
        return stmt;
    }

    protected void selectNode(SelectNode selectNode) throws StandardException {
        boolean foundSubquery = false;
        for (FromTable fromTable : selectNode.getFromList()) {
            if (fromTable instanceof FromSubquery) {
                ResultSetNode subquery = ((FromSubquery)fromTable).getSubquery();
                if (subquery instanceof SelectNode) {
                    selectNode((SelectNode)subquery);
                }
                foundSubquery = true;
            }
        }
        // May have eliminated from subquery, but can't from main one.
        if (foundSubquery) return;
        
        // Nothing more to do if not now distinct.
        if (!selectNode.isDistinct()) return;

        ResultColumnList resultColumns = selectNode.getResultColumns();
        AndNode whereConditions = (AndNode)selectNode.getWhereClause();
        for (FromTable fromTable : selectNode.getFromList()) {
            if (!isTableDistinct(fromTable, resultColumns, whereConditions, null))
                return;
        }

        // Everything looks distinct already.
        selectNode.clearDistinct();
    }

    protected boolean isTableDistinct(FromTable fromTable,
                                      ResultColumnList resultColumns, 
                                      AndNode whereConditions, AndNode joinConditions)
            throws StandardException {
        if (fromTable instanceof FromBaseTable) {
            TableBinding binding = (TableBinding)fromTable.getUserData();
            if (binding == null) return false;

            return isTableDistinct((FromBaseTable)fromTable, binding, 
                                   resultColumns, whereConditions, joinConditions);
        }
        else if (fromTable instanceof JoinNode) {
            return isJoinDistinct((JoinNode)fromTable, resultColumns, whereConditions);
        }
        else
            return false;
    }

    protected boolean isJoinDistinct(JoinNode join,
                                     ResultColumnList resultColumns, 
                                     AndNode whereConditions)  
            throws StandardException {
        ResultSetNode left = join.getLeftResultSet();
        ResultSetNode right = join.getRightResultSet();
        if (!((left instanceof FromTable) && (right instanceof FromTable)))
            return false;
        FromTable leftTable = (FromTable)left;
        FromTable rightTable = (FromTable)right;
        ValueNode joinClause = join.getJoinClause();
        if ((joinClause != null) && !(joinClause instanceof AndNode))
            return false;
        AndNode joinConditions = (AndNode)joinClause;
        if (join instanceof HalfOuterJoinNode) {
            if (((HalfOuterJoinNode)join).isRightOuterJoin()) {
                return (isTableDistinct(leftTable, resultColumns, 
                                        null, joinConditions) &&
                        isTableDistinct(rightTable, resultColumns, 
                                        whereConditions, null));
            }
            else {
                return (isTableDistinct(leftTable, resultColumns, 
                                        whereConditions, null) &&
                        isTableDistinct(rightTable, resultColumns, 
                                        null, joinConditions));
            }
        }
        else
            return (isTableDistinct(leftTable, resultColumns, 
                                    whereConditions, joinConditions) &&
                    isTableDistinct(rightTable, resultColumns, 
                                    whereConditions, joinConditions));
    }

    protected boolean isTableDistinct(FromBaseTable table, TableBinding binding,
                                      ResultColumnList resultColumns, 
                                      AndNode whereConditions, AndNode joinConditions)
            throws StandardException {
        return true;
    }

}
