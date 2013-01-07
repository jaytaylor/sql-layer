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

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.CopyStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.externaldata.CsvFormat;

import static com.akiban.sql.pg.PostgresCopyInStatement.csvFormat;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.io.*;
import java.util.*;

/** COPY ... TO */
public class PostgresCopyOutStatement extends PostgresOperatorStatement
{
    private File toFile;
    private CsvFormat csvFormat;

    public PostgresCopyOutStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        CopyStatementNode copyStmt = (CopyStatementNode)stmt;
        try {
            stmt = copyStmt.asQuery();
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }        
        PostgresStatement pstmt = super.finishGenerating(server, sql, stmt,
                                                         params, paramTypes);
        assert (pstmt == this);
        if (copyStmt.getFilename() != null)
            toFile = new File(copyStmt.getFilename());
        CopyStatementNode.Format format = copyStmt.getFormat();
        if (format == null)
            format = CopyStatementNode.Format.CSV;
        switch (format) {
        case CSV:
            csvFormat = csvFormat(copyStmt, server);
            if (copyStmt.isHeader()) {
                csvFormat.setHeadings(getColumnNames());
            }
            break;
        default:
            throw new UnsupportedSQLException("COPY FORMAT " + format);
        }
        return this;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        if (toFile == null)
            return super.execute(context, maxrows);

        PostgresServerSession server = context.getServer();
        int nrows = 0;
        Cursor cursor = null;
        OutputStream outputStream = null;
        boolean lockSuccess = false;
        try {
            lock(context, DXLFunction.UNSPECIFIED_DML_READ);
            lockSuccess = true;
            cursor = context.startCursor(this);
            outputStream = new FileOutputStream(toFile);
            int ncols = getColumnTypes().size();
            PostgresCopyCsvOutputter outputter = 
                new PostgresCopyCsvOutputter(context, this, csvFormat);
            if (csvFormat.getHeadings() != null) {
                outputter.outputHeadings(outputStream);
                nrows++;
            }
            Row row;
            while ((row = cursor.next()) != null) {
                outputter.output(row, outputStream, usesPValues());
                nrows++;
            }
        }
        finally {
            if (outputStream != null)
                outputStream.close();
            context.finishCursor(this, cursor, false);
            unlock(context, DXLFunction.UNSPECIFIED_DML_READ, lockSuccess);
        }
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("COPY " + nrows);
            messenger.sendMessage();
        }
        return 0;
    }

    @Override
    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresCopyCsvOutputter(context, this, csvFormat);
    }
    
    @Override
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
    }

    @Override
    public boolean putInCache() {
        return false;
    }

}
