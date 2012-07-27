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

package com.akiban.sql.pg;

import com.akiban.sql.server.ServerOperatorCompiler;
import com.akiban.sql.server.ServerPlanContext;

import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.PhysicalSelect;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.PhysicalUpdate;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.server.error.SQLParseException;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.server.service.EventTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Compile SQL SELECT statements into operator trees if possible.
 */
public class PostgresOperatorCompiler extends ServerOperatorCompiler
                                      implements PostgresStatementGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresOperatorCompiler.class);

    protected PostgresOperatorCompiler() {
    }

    public static PostgresOperatorCompiler create(PostgresServerSession server) {
        PostgresOperatorCompiler compiler = new PostgresOperatorCompiler();
        compiler.initServer(server);
        compiler.initDone();
        return compiler;
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
        catch (SQLParserException ex) {
            throw new SQLParseException(ex);
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
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

        @Override
        public String toString() {
            return super.toString() + ":" + type;
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
        PlanContext planContext = new ServerPlanContext(this, new PostgresQueryContext(session));
        BasePlannable result = null;
        tracer = session.getSessionTracer(); // Don't think this ever changes.
        try {
            tracer.beginEvent(EventTypes.COMPILE);
            result = compile(dmlStmt, params, planContext);
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

        if (result.isUpdate())
            return generateUpdate((PhysicalUpdate)result, stmt.statementToString(),
                                  parameterTypes);
        else
            return generateSelect((PhysicalSelect)result,
                                  parameterTypes);
    }

    protected PostgresStatement generateUpdate(PhysicalUpdate update, String statementType,
                                               PostgresType[] parameterTypes) {
        return new PostgresModifyOperatorStatement(statementType,
                                                   update.getUpdatePlannable(),
                                                   parameterTypes,
                                                   usesPValues());
    }

    protected PostgresStatement generateSelect(PhysicalSelect select,
                                               PostgresType[] parameterTypes) {
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
                                             parameterTypes,
                                             usesPValues());
    }

}
