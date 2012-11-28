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

import com.akiban.sql.optimizer.OperatorCompiler;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.rule.ExplainPlanContext;
import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.ExplainStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.server.ServerValueEncoder;

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
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
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
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        ServerValueEncoder encoder = new ServerValueEncoder(messenger.getEncoding());
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
        ExplainPlanContext context = new ExplainPlanContext(compiler);
        StatementNode innerStmt = ((ExplainStatementNode)stmt).getStatement();
        Explainable explainable;
        if (innerStmt instanceof CallStatementNode) {
            explainable = PostgresCallStatementGenerator.explainable(server, (CallStatementNode)stmt, params, paramTypes);
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
            DefaultFormatter f = new DefaultFormatter(server.getDefaultSchemaName(), true);
            explain = f.format(explainable.getExplainer(context.getExplainContext()));
        }
        init(explain, compiler.usesPValues());
        compiler = null;
        return this;
    }
}
