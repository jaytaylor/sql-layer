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

import com.akiban.sql.StandardException;

import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.PhysicalSelect;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.PhysicalUpdate;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.rule.BaseRule;

import com.akiban.sql.parser.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.ais.model.Column;

import com.akiban.server.error.ParseException;
import com.akiban.server.service.EventTypes;
import com.akiban.server.service.instrumentation.SessionTracer;

import com.akiban.server.expression.EnvironmentExpressionSetting;

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

    private SessionTracer tracer;

    public PostgresOperatorCompiler(PostgresServerSession server) {
        super(server.getParser(), server.getAIS(), server.getDefaultSchemaName(),
              server.functionsRegistry(), server.indexEstimator());

        server.setAttribute("aisBinder", binder);
        server.setAttribute("compiler", this);

        tracer = server.getSessionTracer();
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes)  {
        // This very inefficient reparsing by every generator is actually avoided.
        SQLParser parser = server.getParser();
        try {
            return generate(server, parser.parseStatement(sql), 
                            parser.getParameterList(), paramTypes);
        } 
        catch (StandardException e) {
            throw new ParseException("", e.getMessage(), sql);
        }
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
        binder.setDefaultSchemaName(server.getDefaultSchemaName());
    }

    @Override
    protected DMLStatementNode bindAndTransform(DMLStatementNode stmt)  {
        try {
            tracer.beginEvent(EventTypes.BIND_AND_GROUP); // TODO: rename.
            return super.bindAndTransform(stmt);
        } 
        finally {
            tracer.endEvent();
        }
    }

    static class PostgresResultColumn extends PhysicalResultColumn {
        private PostgresType type;
        
        public PostgresResultColumn(String name, PostgresType type) {
            super(name);
            this.type = type;
        }

        public PostgresType getType() {
            return type;
        }
    }

    @Override
    public PhysicalResultColumn getResultColumn(ResultField field) {
        PostgresType pgType = null;
        if (field.getAIScolumn() != null) {
            pgType = PostgresType.fromAIS(field.getAIScolumn());
        }
        else if (field.getSQLtype() != null) {
            pgType = PostgresType.fromDerby(field.getSQLtype());
        }
        return new PostgresResultColumn(field.getName(), pgType);
    }

    @Override
    public PostgresStatement generate(PostgresServerSession session,
                                      StatementNode stmt, 
                                      List<ParameterNode> params, int[] paramTypes) {
        if (stmt instanceof CallStatementNode || !(stmt instanceof DMLStatementNode))
            return null;
        DMLStatementNode dmlStmt = (DMLStatementNode)stmt;
        BasePlannable result = null;
        tracer = session.getSessionTracer(); // Don't think this ever changes.
        try {
            tracer.beginEvent(EventTypes.COMPILE);
            result = compile(dmlStmt, params);
        } 
        finally {
            session.getSessionTracer().endEvent();
        }

        logger.debug("Operator:\n{}", result);

        PostgresType[] parameterTypes = null;
        if (result.getParameterTypes() != null) {
            DataTypeDescriptor[] sqlTypes = result.getParameterTypes();
            int nparams = sqlTypes.length;
            parameterTypes = new PostgresType[nparams];
            for (int i = 0; i < nparams; i++) {
                DataTypeDescriptor sqlType = sqlTypes[i];
                if (sqlType != null)
                    parameterTypes[i] = PostgresType.fromDerby(sqlType);
            }
        }

        List<EnvironmentExpressionSetting> environmentSettings = result.getEnvironmentSettings();        

        if (result.isUpdate()) {
            PhysicalUpdate update = (PhysicalUpdate)result;
            return new PostgresModifyOperatorStatement(stmt.statementToString(),
                                                       update.getUpdatePlannable(),
                                                       parameterTypes, environmentSettings);
        }
        else {
            PhysicalSelect select = (PhysicalSelect)result;
            int ncols = select.getResultColumns().size();
            List<String> columnNames = new ArrayList<String>(ncols);
            List<PostgresType> columnTypes = new ArrayList<PostgresType>(ncols);
            for (PhysicalResultColumn physColumn : select.getResultColumns()) {
                PostgresResultColumn resultColumn = (PostgresResultColumn)physColumn;
                columnNames.add(resultColumn.getName());
                columnTypes.add(resultColumn.getType());
            }
            return new PostgresOperatorStatement(select.getResultOperator(),
                                                 select.getResultRowType(),
                                                 columnNames, columnTypes,
                                                 parameterTypes, environmentSettings);
        }
    }

    @Override
    public void beginRule(BaseRule rule) {
        tracer.beginEvent(rule.getTraceName());
    }

    @Override
    public void endRule(BaseRule rule) {
        tracer.endEvent();
    }

}
