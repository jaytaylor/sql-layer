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

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.PhysicalSelect;
import com.akiban.sql.optimizer.plan.PhysicalUpdate;
import com.akiban.sql.optimizer.rule.PlanContext;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.server.ServerPlanContext;
import com.akiban.sql.types.DataTypeDescriptor;

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
        PlanContext planContext = new ServerPlanContext(compiler, new PostgresQueryContext(server));
        BasePlannable result = compiler.compile(dmlStmt, params, planContext);

        PostgresType[] parameterTypes = null;
        if (result.getParameterTypes() != null) {
            DataTypeDescriptor[] sqlTypes = result.getParameterTypes();
            int nparams = sqlTypes.length;
            parameterTypes = new PostgresType[nparams];
            for (int i = 0; i < nparams; i++) {
                PostgresType pgType = null;
                DataTypeDescriptor sqlType = sqlTypes[i];
                if (sqlType != null) {
                    AkType akType = TypesTranslation.sqlTypeToAkType(sqlType);
                    TInstance tInstance = TypesTranslation.toTInstance(sqlType);
                    pgType = PostgresType.fromDerby(sqlType, akType, tInstance);
                }
                if ((paramTypes != null) && (i < paramTypes.length)) {
                    // Make a type that has the target that the query wants, with the
                    // OID that the client proposed to send so that we
                    // decode it properly.
                    PostgresType.TypeOid oid = PostgresType.TypeOid.fromOid(paramTypes[i]);
                    if (oid != null) {
                        if (pgType == null)
                            pgType = new PostgresType(oid, (short)-1, -1, null, null);
                        else
                            pgType = new PostgresType(oid,  (short)-1, -1,
                                                      pgType.getAkType(),
                                                      pgType.getInstance());
                    }
                }
                parameterTypes[i] = pgType;
            }
        }

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
}
