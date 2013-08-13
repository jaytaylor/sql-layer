/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.pg;

import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.CostEstimate;
import com.akiban.sql.optimizer.rule.ExplainPlanContext;
import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ExplainStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.server.ServerValueEncoder;

import com.akiban.qp.operator.QueryBindings;

import com.akiban.server.explain.Explainable;
import com.akiban.server.explain.format.DefaultFormatter;
import com.akiban.server.explain.format.JsonFormatter;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.mcompat.mtypes.MString;

import java.util.Collections;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** SQL statement to explain another one. */
public class PostgresExplainStatement implements PostgresStatement
{
    private OperatorCompiler compiler; // Used only to finish generation
    private List<String> explanation;
    private String colName;
    private PostgresType colType;
    private boolean usePVals;
    private long aisGeneration;

    public PostgresExplainStatement(OperatorCompiler compiler) {
        this.compiler = compiler;
    }

    public void init(List<String> explanation, boolean usePVals) {
        this.explanation = explanation;

        int maxlen = 32;
        for (String row : explanation) {
            if (maxlen < row.length())
                maxlen = row.length();
        }
        colName = "OPERATORS";
        colType = new PostgresType(PostgresType.TypeOid.VARCHAR_TYPE_OID, (short)-1, maxlen,
                                   AkType.VARCHAR, MString.VARCHAR.instance(maxlen, false));
        this.usePVals = usePVals;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        if (params) {
            messenger.beginMessage(PostgresMessages.PARAMETER_DESCRIPTION_TYPE.code());
            messenger.writeShort(0);
            messenger.sendMessage();
        }
        messenger.beginMessage(PostgresMessages.ROW_DESCRIPTION_TYPE.code());
        messenger.writeShort(1);
        messenger.writeString(colName); // attname
        messenger.writeInt(0);    // attrelid
        messenger.writeShort(0);  // attnum
        messenger.writeInt(colType.getOid()); // atttypid
        messenger.writeShort(colType.getLength()); // attlen
        messenger.writeInt(colType.getModifier()); // atttypmod
        messenger.writeShort(0);
        messenger.sendMessage();
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        ServerValueEncoder encoder = server.getValueEncoder();
        int nrows = 0;
        for (String row : explanation) {
            messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
            messenger.writeShort(1);
            ByteArrayOutputStream bytes;
            if (usePVals) bytes = encoder.encodePObject(row, colType, false);
            else bytes = encoder.encodeObject(row, colType, false);
            messenger.writeInt(bytes.size());
            messenger.writeByteStream(bytes);
            messenger.sendMessage();
            nrows++;
            if ((maxrows > 0) && (nrows >= maxrows))
                break;
        }
        {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("EXPLAIN " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

    @Override
    public boolean hasAISGeneration() {
        return aisGeneration != 0;
    }

    @Override
    public void setAISGeneration(long aisGeneration) {
        this.aisGeneration = aisGeneration;
    }

    @Override
    public long getAISGeneration() {
        return aisGeneration;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server, String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        ExplainPlanContext context = new ExplainPlanContext(compiler, server.getServiceManager(), server.getSession());
        ExplainStatementNode explainStmt = (ExplainStatementNode)stmt;
        StatementNode innerStmt = explainStmt.getStatement();
        Explainable explainable;
        if (innerStmt instanceof CallStatementNode) {
            explainable = PostgresCallStatementGenerator.explainable(server, (CallStatementNode)innerStmt, params, paramTypes);
        }
        else {
            BasePlannable result = compiler.compile((DMLStatementNode)innerStmt, params, context);
            explainable = result.getPlannable();
        }
        List<String> explain;
        if (compiler instanceof PostgresJsonCompiler) {
            JsonFormatter f = new JsonFormatter();
            explain = Collections.singletonList(f.format(explainable.getExplainer(context.getExplainContext())));
        }
        else {
            DefaultFormatter.LevelOfDetail detail;
            switch (explainStmt.getDetail()) {
            case BRIEF:
                detail = DefaultFormatter.LevelOfDetail.BRIEF;
                break;
            default:
            case NORMAL:
                detail = DefaultFormatter.LevelOfDetail.NORMAL;
                break;
            case VERBOSE:
                detail = DefaultFormatter.LevelOfDetail.VERBOSE;
                break;
            }
            DefaultFormatter f = new DefaultFormatter(server.getDefaultSchemaName(), detail);
            explain = f.format(explainable.getExplainer(context.getExplainContext()));
        }
        init(explain, compiler.usesPValues());
        compiler = null;
        return this;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    public CostEstimate getCostEstimate() {
        return null;
    }

}
