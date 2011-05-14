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

package com.akiban.sql.pg;

import com.akiban.sql.parser.*;
import com.akiban.sql.compiler.*;
import com.akiban.sql.optimizer.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.HapiPredicate;

import java.util.*;

/**
 * Compile SQL SELECT statements into Hapi requests if possible.
 * Restrictions are:
 * <ul>
 * <li>All result columns directly from tables.</li>
 * <li>All FROM tables in a strict hierarchy with no branching.</li>
 * <li>WHERE or JOIN ON clause has necessary pkey-fkey equality conditions.</li>
 * <li>Remaining WHERE clause is simple boolean predicates between columns from a single table with constants.</li>
 * <li>No (unflattened) subqueries.</li>
 * <li>No ORDER BY.</li>
 * <li>No DISTINCT.</li>
 * <li>No GROUP BY.</li>
 * <li>No OFFSET or FETCH.</li>
 * <li>No WINDOW.</li>
 * <li>No FOR UPDATE.</li>
 * </ul>
 */
public class PostgresHapiCompiler implements PostgresStatementGenerator
{
    private SQLParserContext parserContext;
    private NodeFactory nodeFactory;
    private AISBinder binder;
    private AISTypeComputer typeComputer;
    private BooleanNormalizer booleanNormalizer;
    private SubqueryFlattener subqueryFlattener;
    private Grouper grouper;

    public PostgresHapiCompiler(SQLParser parser, 
                                AkibanInformationSchema ais, String defaultSchemaName) {
        parserContext = parser;
        nodeFactory = parserContext.getNodeFactory();
        binder = new AISBinder(ais, defaultSchemaName);
        parser.setNodeFactory(new BindingNodeFactory(nodeFactory));
        typeComputer = new AISTypeComputer();
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        grouper = new Grouper(parser);
    }

    public void addView(ViewDefinition view) throws StandardException {
        binder.addView(view);
    }

    @Override
    public PostgresStatement generate(StatementNode stmt, int[] paramTypes)
            throws StandardException {
        if (!(stmt instanceof CursorNode))
            return null;
        CursorNode cursor = (CursorNode)stmt;

        // Get into bound & grouped form.
        binder.bind(cursor);
        cursor = (CursorNode)booleanNormalizer.normalize(cursor);
        typeComputer.compute(cursor);
        cursor = (CursorNode)subqueryFlattener.flatten(cursor);
        grouper.group(cursor);

        if (cursor.getOrderByList() != null)
            throw new StandardException("Unsupported ORDER BY");
        if (cursor.getOffsetClause() != null)
            throw new StandardException("Unsupported OFFSET");
        if (cursor.getFetchFirstClause() != null)
            throw new StandardException("Unsupported FETCH");
        if (cursor.getUpdateMode() == CursorNode.UpdateMode.UPDATE)
            throw new StandardException("Unsupported FOR UPDATE");

        SelectNode select = (SelectNode)cursor.getResultSetNode();
        if (select.getGroupByList() != null)
            throw new StandardException("Unsupported GROUP BY");
        if (select.isDistinct())
            throw new StandardException("Unsupported DISTINCT");
        if (select.hasWindows())
            throw new StandardException("Unsupported WINDOW");

        UserTable shallowestTable = null, queryTable = null, deepestTable = null;
        List<HapiPredicate> predicates = new ArrayList<HapiPredicate>();
        List<Column> columns = new ArrayList<Column>();

        GroupBinding group = null;
        for (FromTable fromTable : select.getFromList()) {
            if (!(fromTable instanceof FromBaseTable))
                throw new StandardException("Unsupported FROM non-table: " + fromTable);
            TableBinding tb = (TableBinding)fromTable.getUserData();
            if (tb == null) 
                throw new StandardException("Unsupported FROM table: " + fromTable);
            GroupBinding gb = tb.getGroupBinding();
            if (gb == null)
                throw new StandardException("Unsupported FROM non-group: " + fromTable);
            if (group == null)
                group = gb;
            else if (group != gb)
                throw new StandardException("Unsupported multiple groups");
            UserTable table = (UserTable)tb.getTable();
            checkDepth:
            if (shallowestTable == null)
                shallowestTable = deepestTable = table;
            else {
                if (table != shallowestTable) {
                    if (isAncestorTable(table, shallowestTable))
                        shallowestTable = table; // Going shallower
                    else if (isAncestorTable(shallowestTable, table)) {
                        if (shallowestTable == deepestTable) {
                            deepestTable = table; // Going deeper
                            break checkDepth;
                        }
                    }
                    else
                        throw new StandardException("Unsupported branching group");
                }
                if (table != deepestTable) {
                    if (isAncestorTable(deepestTable, table))
                        deepestTable = table; // Going deeper
                    else if (!isAncestorTable(table, deepestTable))
                        throw new StandardException("Unsupported branching group");
                }
            }
        }

        for (ResultColumn result : select.getResultColumns()) {
            if (!(result.getExpression() instanceof ColumnReference))
                throw new StandardException("Unsupported result column: " + result);
            ColumnReference cref = (ColumnReference)result.getExpression();
            ColumnBinding cb = (ColumnBinding)cref.getUserData();
            if (cb == null)
                throw new StandardException("Unsupported result column: " + result);
            Column column = cb.getColumn();
            if (column == null)
                throw new StandardException("Unsupported result column: " + result);
            columns.add(column);
        }

        ValueNode whereClause = select.getWhereClause();
        while (whereClause != null) {
            if (whereClause.isBooleanTrue()) break;
            if (!(whereClause instanceof AndNode))
                throw new StandardException("Unsupported complex WHERE");
            AndNode andNode = (AndNode)whereClause;
            whereClause = andNode.getRightOperand();
            ValueNode condition = andNode.getLeftOperand();
            if (grouper.getJoinConditions().contains(condition))
                continue;
            HapiPredicate.Operator op;
            switch (condition.getNodeType()) {
            case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
                op = HapiPredicate.Operator.EQ;
                break;
            case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
                op = HapiPredicate.Operator.GT;
                break;
            case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
                op = HapiPredicate.Operator.GTE;
                break;
            case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
                op = HapiPredicate.Operator.LT;
                break;
            case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
                op = HapiPredicate.Operator.LTE;
                break;
            default:
                throw new StandardException("Unsupported WHERE predicate");
            }
            BinaryOperatorNode binop = (BinaryOperatorNode)condition;
            ValueNode leftOperand = binop.getLeftOperand();
            ValueNode rightOperand = binop.getRightOperand();
            Column column;
            if (!(leftOperand instanceof ColumnReference) ||
                (leftOperand.getUserData() == null))
                throw new StandardException("Unsupported WHERE predicate on non-column");
            column = ((ColumnBinding)leftOperand.getUserData()).getColumn();
            if (column == null)
                throw new StandardException("Unsupported WHERE predicate on non-column");
            UserTable predTable = column.getUserTable();
            if (queryTable == null)
                queryTable = predTable;
            else if (queryTable != predTable)
                throw new StandardException("Unsupported WHERE predicate on multiple tables");
            if (rightOperand instanceof ConstantNode)
                predicates.add(new PostgresHapiPredicate(column, op,
                                                         ((ConstantNode)
                                                          rightOperand).getValue().toString()));
            else if (rightOperand instanceof ParameterNode)
                predicates.add(new PostgresHapiPredicate(column, op,
                                                         ((ParameterNode)
                                                          rightOperand).getParameterNumber()));
            else
                throw new StandardException("Unsupported WHERE predicate on non-constant");
        }

        if (queryTable == null)
            queryTable = deepestTable;

        return new PostgresHapiRequest(shallowestTable, queryTable, deepestTable,
                                       predicates, columns);
    }

    /** Is t1 an ancestor of t2? */
    static boolean isAncestorTable(UserTable t1, UserTable t2) {
        while (true) {
            Join j = t2.getParentJoin();
            if (j == null)
                return false;
            UserTable parent = j.getParent();
            if (parent == null)
                return false;
            if (parent == t1)
                return true;
            t2 = parent;
        }
    }
}
