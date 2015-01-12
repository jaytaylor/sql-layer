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

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.CopyStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.externaldata.CsvFormat;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;

import static com.foundationdb.sql.pg.PostgresCopyInStatement.csvFormat;
import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

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
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        if (toFile == null)
            return super.execute(context, bindings, maxrows);

        PostgresServerSession server = context.getServer();
        server.getSessionMonitor().countEvent(StatementTypes.OTHER_STMT);
        int nrows = 0;
        Cursor cursor = null;
        OutputStream outputStream = null;
        try {
            preExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
            cursor = context.startCursor(this, bindings);
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
                outputter.output(row, outputStream);
                nrows++;
            }
        }
        finally {
            if (outputStream != null)
                outputStream.close();
            context.finishCursor(this, cursor, nrows, false);
            postExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
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
