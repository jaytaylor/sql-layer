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
            context.finishCursor(this, cursor, nrows, false);
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
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
    }

    @Override
    public boolean putInCache() {
        return false;
    }

}
