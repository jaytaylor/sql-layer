/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.pg;

import com.foundationdb.server.error.UnknownDataTypeException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.optimizer.plan.BasePlannable;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect;
import com.foundationdb.sql.optimizer.plan.PhysicalUpdate;
import com.foundationdb.sql.optimizer.rule.PlanContext;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerPlanContext;

import java.util.List;

public abstract class PostgresBaseOperatorStatement extends PostgresDMLStatement
{
    private PostgresOperatorCompiler compiler;

    protected PostgresBaseOperatorStatement(PostgresOperatorCompiler compiler) {
        this.compiler = compiler;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        DMLStatementNode dmlStmt = (DMLStatementNode)stmt;
        PostgresQueryContext queryContext = new PostgresQueryContext(server);
        PlanContext planContext = new ServerPlanContext(compiler, queryContext);
        // TODO: This needs to make types with better default attributes or else
        // decimals and strings get truncated, collation doesn't match, etc.
        if (paramTypes != null && false) {
            for (ParameterNode param : params) {
                int paramno = param.getParameterNumber();
                if (paramno < paramTypes.length) {
                    TInstance type = null;
                    try {
                        type = server.typesTranslator().typeClassForJDBCType(PostgresType.toJDBC(paramTypes[paramno])).instance(true);
                    }
                    catch (UnknownDataTypeException ex) {
                        server.warnClient(ex);
                    }
                    param.setUserData(type);
                }
            }
        }
        BasePlannable result = compiler.compile(dmlStmt, params, planContext);
        PostgresType[] parameterTypes = getParameterTypes(result.getParameterTypes(),
                                                          paramTypes,
                                                          server.typesTranslator());

        final PostgresBaseOperatorStatement pbos;
        if (result.isUpdate())
            pbos = compiler.generateUpdate(this,
                                           (PhysicalUpdate)result, stmt.statementToString(),
                                           parameterTypes);
        else
            pbos = compiler.generateSelect(this,
                                           (PhysicalSelect)result,
                                           parameterTypes);
        pbos.compiler = null;
        return pbos;
    }

    protected PostgresType[] getParameterTypes(BasePlannable.ParameterType[] planTypes,
                                               int[] paramTypes,
                                               TypesTranslator typesTranslator) {
        if (planTypes == null) 
            return null;
        int nparams = planTypes.length;
        PostgresType[] parameterTypes = new PostgresType[nparams];
        for (int i = 0; i < nparams; i++) {
            BasePlannable.ParameterType planType = planTypes[i];
            PostgresType pgType = null;
            if ((planType != null) && (planType.getType() != null)) {
                pgType = PostgresType.fromTInstance(planType.getType());
            }
            if ((paramTypes != null) && (i < paramTypes.length)) {
                // Make a type that has the target that the query wants, with the
                // OID that the client proposed to send so that we
                // decode it properly.
                PostgresType.TypeOid oid = PostgresType.TypeOid.fromOid(paramTypes[i]);
                if (oid != null) {
                    if (pgType == null)
                        pgType = new PostgresType(oid, (short)-1, -1, null);
                    else
                        pgType = new PostgresType(oid,  (short)-1, -1,
                                                  pgType.getType());
                }
            }
            parameterTypes[i] = pgType;
        }
        return parameterTypes;
    }
}
