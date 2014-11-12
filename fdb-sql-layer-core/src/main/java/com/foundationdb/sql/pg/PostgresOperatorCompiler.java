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

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.sql.server.ServerOperatorCompiler;

import com.foundationdb.sql.optimizer.plan.PhysicalSelect;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.optimizer.plan.PhysicalUpdate;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.*;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.server.error.SQLParseException;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.types.TInstance;

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

    public static PostgresOperatorCompiler create(PostgresServerSession server, KeyCreator keyCreator) {
        PostgresOperatorCompiler compiler = new PostgresOperatorCompiler();
        compiler.initServer(server, keyCreator);
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
        private Column aisColumn;
        
        public PostgresResultColumn(String name, PostgresType type, Column aisColumn) {
            super(name);
            this.type = type;
            this.aisColumn = aisColumn;
        }

        public PostgresType getType() {
            return type;
        }

        public Column getAISColumn() {
            return aisColumn;
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
            TInstance type = field.getType();
            if (type == null)
                type = getTypesTranslator().typeForSQLType(sqlType);
            pgType = PostgresType.fromDerby(sqlType, type);
        }
        else {
            // TODO a fourth usage of null, the postgres unknown type. I don't believe parameter or null is the same
            // although parameter might fit under this.
            pgType = new PostgresType(PostgresType.TypeOid.UNKNOWN_TYPE_OID,
                                      (short)-1, -1, null);
        }
        return new PostgresResultColumn(field.getName(), pgType, field.getAIScolumn());
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
            List<String> columnNames = new ArrayList<>(ncols);
            List<PostgresType> columnTypes = new ArrayList<>(ncols);
            List<Column> aisColumns = new ArrayList<>(ncols);
            for (PhysicalResultColumn physColumn : update.getResultColumns()) {
                PostgresResultColumn resultColumn = (PostgresResultColumn)physColumn;
                columnNames.add(resultColumn.getName());
                columnTypes.add(resultColumn.getType());
                aisColumns.add(resultColumn.getAISColumn());
            }

            pmstmt.init(statementType,
                        (Operator) update.getPlannable(),
                        update.getResultRowType(),
                        columnNames, columnTypes, aisColumns,
                        parameterTypes,
                        update.getCostEstimate(),
                        update.putInCache());
        } else { 
            pmstmt.init(statementType,
                        (Operator)update.getPlannable(),
                        parameterTypes,
                        update.getCostEstimate(),
                        update.putInCache());
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
        List<String> columnNames = new ArrayList<>(ncols);
        List<PostgresType> columnTypes = new ArrayList<>(ncols);
        List<Column> aisColumns = new ArrayList<>(ncols);
        for (PhysicalResultColumn physColumn : select.getResultColumns()) {
            PostgresResultColumn resultColumn = (PostgresResultColumn)physColumn;
            columnNames.add(resultColumn.getName());
            columnTypes.add(resultColumn.getType());
            aisColumns.add(resultColumn.getAISColumn());
        }
        postmt.init(select.getResultOperator(),
                    select.getResultRowType(),
                    columnNames, columnTypes, aisColumns,
                    parameterTypes,
                    select.getCostEstimate());
        return postmt;
    }
}
