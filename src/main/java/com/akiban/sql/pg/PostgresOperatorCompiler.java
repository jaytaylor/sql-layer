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

import com.akiban.qp.operator.Operator;
import com.akiban.sql.server.ServerOperatorCompiler;

import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.PhysicalSelect;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.PhysicalUpdate;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.server.error.SQLParseException;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

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
            return generateStub(server, sql, parser.parseStatement(sql),
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
            DataTypeDescriptor sqlType = field.getSQLtype();
            AkType akType = TypesTranslation.sqlTypeToAkType(sqlType);
            TInstance tInstance = field.getTInstance();
            if (tInstance == null)
                tInstance = TypesTranslation.toTInstance(sqlType);
            pgType = PostgresType.fromDerby(sqlType, akType, tInstance);
        }
        else {
            pgType = new PostgresType(PostgresType.TypeOid.UNKNOWN_TYPE_OID,
                                      (short)-1, -1, null, null);
        }
        return new PostgresResultColumn(field.getName(), pgType);
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession session,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes) {
        if (stmt instanceof CallStatementNode || !(stmt instanceof DMLStatementNode))
            return null;
        // Extremely similar to ASTStatementLoader.Loader#toStatement()
        switch(stmt.getNodeType()) {
            case NodeTypes.CURSOR_NODE:
                return generateSelect();
            case NodeTypes.DELETE_NODE:
            case NodeTypes.UPDATE_NODE:
            case NodeTypes.INSERT_NODE:
                return generateUpdate();
            default:
                throw new SQLParserInternalException(
                        new StandardException("Unsupported statement type: " + stmt.statementToString())
                );
        }
    }

    protected PostgresBaseOperatorStatement generateUpdate() {
        return new PostgresModifyOperatorStatement(this);
    }

    protected PostgresBaseOperatorStatement generateUpdate(PostgresStatement pstmt,
                                                           PhysicalUpdate update, String statementType,
                                                           PostgresType[] parameterTypes) {
        PostgresModifyOperatorStatement pmstmt = (PostgresModifyOperatorStatement)pstmt;
        if (update.isReturning()) {
            int ncols = update.getResultColumns().size();
            List<String> columnNames = new ArrayList<String>(ncols);
            List<PostgresType> columnTypes = new ArrayList<PostgresType>(ncols);
            for (PhysicalResultColumn physColumn : update.getResultColumns()) {
                PostgresResultColumn resultColumn = (PostgresResultColumn)physColumn;
                columnNames.add(resultColumn.getName());
                columnTypes.add(resultColumn.getType());
            }

            pmstmt.init(statementType,
                        (Operator) update.getPlannable(),
                        update.getResultRowType(),
                        columnNames, columnTypes,
                        parameterTypes,
                        usesPValues(),
                        update.isRequireStepIsolation());
        } else { 
            pmstmt.init(statementType,
                        (Operator)update.getPlannable(),
                        parameterTypes,
                        usesPValues(),
                        update.isRequireStepIsolation());
        }
        return pmstmt;
    }

    protected PostgresBaseOperatorStatement generateSelect() {
        return new PostgresOperatorStatement(this);
    }

    protected PostgresBaseOperatorStatement generateSelect(PostgresStatement pstmt,
                                                           PhysicalSelect select,
                                                           PostgresType[] parameterTypes) {
        PostgresOperatorStatement postmt = (PostgresOperatorStatement)pstmt;

        int ncols = select.getResultColumns().size();
        List<String> columnNames = new ArrayList<String>(ncols);
        List<PostgresType> columnTypes = new ArrayList<PostgresType>(ncols);
        for (PhysicalResultColumn physColumn : select.getResultColumns()) {
            PostgresResultColumn resultColumn = (PostgresResultColumn)physColumn;
            columnNames.add(resultColumn.getName());
            columnTypes.add(resultColumn.getType());
        }
        postmt.init(select.getResultOperator(),
                    select.getResultRowType(),
                    columnNames, columnTypes,
                    parameterTypes,
                    usesPValues());
        return postmt;
    }
}
