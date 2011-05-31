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

import com.akiban.ais.model.TableIndex;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.sql.StandardException;

import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.ExpressionRow;

import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;

import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;

import com.akiban.qp.expression.Expression;

import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitGroupRow;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Compile SQL SELECT statements into operator trees if possible.
 */
public class PostgresOperatorCompiler extends OperatorCompiler
                                      implements PostgresStatementGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresOperatorCompiler.class);

    private PersistitAdapter adapter;

    public PostgresOperatorCompiler(PostgresServerSession server) {
        super(server.getParser(), server.getAIS(), server.getDefaultSchemaName());
        Store store = server.getServiceManager().getStore();
        PersistitStore persistitStore;
        if (store instanceof OperatorStore)
            persistitStore = ((OperatorStore)store).getPersistitStore();
        else
            persistitStore = (PersistitStore)store;
        adapter = new PersistitAdapter(schema, persistitStore, server.getSession());

        server.setAttribute("aisBinder", binder);
        server.setAttribute("compiler", this);
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes) 
            throws StandardException {
        // This very inefficient reparsing by every generator is actually avoided.
        return generate(server, server.getParser().parseStatement(sql), paramTypes);
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
        binder.setDefaultSchemaName(server.getDefaultSchemaName());
    }

    @Override
    public PostgresStatement generate(PostgresServerSession session,
                                      StatementNode stmt, int[] paramTypes)
            throws StandardException {
        if (!(stmt instanceof DMLStatementNode))
            return null;
        DMLStatementNode dmlStmt = (DMLStatementNode)stmt;
        Result result = null;
        try {
            session.getSessionTracer().beginEvent("sql: optimize: compile");
            result = compile(session.getSessionTracer(), dmlStmt);
        } finally {
            session.getSessionTracer().endEvent();
        }

        logger.debug("Operator:\n{}", result);

        if (result.isModify())
            return new PostgresModifyOperatorStatement(stmt.statementToString(),
                                                       adapter,
                                                       (UpdatePlannable) result.getResultOperator());
        else
            return new PostgresOperatorStatement(adapter,
                                                 (PhysicalOperator) result.getResultOperator(),
                                                 result.getResultRowType(),
                                                 result.getResultColumns(),
                                                 result.getResultColumnOffsets(),
                                                 result.getOffset(),
                                                 result.getLimit());
    }

    // The current implementation of index cursors expects that the
    // key bounds' rows are in the shape of the indexed table, not the
    // index itself.
    protected Row getIndexExpressionRow(TableIndex index, Expression[] keys) {
        UserTable userTable = (UserTable)index.getTable();
        RowType rowType = schema.userTableRowType(userTable);
        Expression[] userKeys = new Expression[rowType.nFields()];
        for (int i = 0; i < keys.length; i++) {
            userKeys[index.getColumns().get(i).getColumn().getPosition()] = keys[i];
        }
        return new ExpressionRow(rowType, userKeys);
    }

}
