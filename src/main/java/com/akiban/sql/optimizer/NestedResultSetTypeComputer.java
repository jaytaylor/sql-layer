/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.server.service.functions.FunctionsRegistry;

public class NestedResultSetTypeComputer extends FunctionsTypeComputer
{
    public NestedResultSetTypeComputer(FunctionsRegistry functionsRegistry) {
        super(functionsRegistry);
    }

    @Override
    public void compute(StatementNode stmt) throws StandardException {
        stmt.accept(this);
        if (stmt.getNodeType() == NodeTypes.CURSOR_NODE) {
            checkForNestedResultSet(((CursorNode)stmt).getResultSetNode());
        }
    }

    protected void checkForNestedResultSet(ResultSetNode resultSet) 
            throws StandardException {
        for (ResultColumn resultColumn : resultSet.getResultColumns()) {
            ValueNode expression = resultColumn.getExpression();
            if (expression.getNodeType() == NodeTypes.SUBQUERY_NODE) {
                SubqueryNode subqueryNode = (SubqueryNode)expression;
                if (subqueryNode.getSubqueryType() == SubqueryNode.SubqueryType.EXPRESSION) {
                    makeNestedResultSet(subqueryNode);
                    resultColumn.setType(subqueryNode.getType());
                }
            }
        }
    }

    protected void makeNestedResultSet(SubqueryNode subqueryNode) 
            throws StandardException {
        ResultSetNode resultSet = subqueryNode.getResultSet();
        checkForNestedResultSet(resultSet); // Do nested first.

        ResultColumnList resultColumns = resultSet.getResultColumns();
        int ncols = resultColumns.size();
        String[] columnNames = new String[ncols];
        DataTypeDescriptor[] columnTypes = new DataTypeDescriptor[ncols];
        for (int i = 0; i < ncols; i++) {
            ResultColumn resultColumn = resultColumns.get(i);
            columnNames[i] = resultColumn.getName();
            columnTypes[i] = resultColumn.getType();
        }
        subqueryNode.setType(DataTypeDescriptor.getRowMultiSet(columnNames, columnTypes));
    }

}
