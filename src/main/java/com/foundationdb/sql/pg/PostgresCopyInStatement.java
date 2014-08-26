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

import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.CopyStatementNode;
import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.externaldata.CsvFormat;
import com.foundationdb.server.service.externaldata.ExternalDataService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.sql.server.ServerTransaction;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/** COPY ... FROM */
public class PostgresCopyInStatement extends PostgresBaseStatement
{
    private Table toTable;
    private List<Column> toColumns;
    private File fromFile;
    private CopyStatementNode.Format format;
    private String encoding;
    private CsvFormat csvFormat;
    private long skipRows;
    private long commitFrequency;
    private int maxRetries;
    private Schema schema;

    private static final Logger logger = LoggerFactory.getLogger(PostgresCopyInStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresCopyInStatement: execute shared");

    protected PostgresCopyInStatement(Schema schema) {
        this.schema = schema;
    }
    

    public static CsvFormat csvFormat(CopyStatementNode copyStmt, 
                                      PostgresServerSession server) {
        String encoding = copyStmt.getEncoding();
        if (encoding == null)
            encoding = server.getMessenger().getEncoding();
        CsvFormat format = new CsvFormat(encoding);
        if (copyStmt.getDelimiter() != null)
            format.setDelimiter(copyStmt.getDelimiter());
        if (copyStmt.getQuote() != null)
            format.setQuote(copyStmt.getQuote());
        if (copyStmt.getEscape() != null)
            format.setEscape(copyStmt.getEscape());
        if (copyStmt.getNullString() != null)
            format.setNullString(copyStmt.getNullString());
        return format;
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        CopyStatementNode copyStmt = (CopyStatementNode)stmt;
        String schemaName = copyStmt.getTableName().getSchemaName();
        String tableName = copyStmt.getTableName().getTableName();
        if (schemaName == null)
            schemaName = server.getDefaultSchemaName();
        toTable = server.getAIS().getTable(schemaName, tableName);
        if (toTable == null)
            throw new NoSuchTableException(schemaName, tableName, 
                                           copyStmt.getTableName());
        if (copyStmt.getColumnList() == null)
            toColumns = toTable.getColumns();
        else {
            toColumns = new ArrayList<>(copyStmt.getColumnList().size());
            for (ResultColumn rc : copyStmt.getColumnList()) {
                ColumnReference cref = rc.getReference();
                Column column = toTable.getColumn(cref.getColumnName());
                if (column == null)
                    throw new NoSuchColumnException(cref.getColumnName(), cref);
                toColumns.add(column);
            }
        }
        if (copyStmt.getFilename() != null)
            fromFile = new File(copyStmt.getFilename());
        format = copyStmt.getFormat();
        if (format == null)
            format = CopyStatementNode.Format.CSV;
        switch (format) {
        case CSV:
            csvFormat = csvFormat(copyStmt, server);
            if (copyStmt.isHeader()) {
                skipRows = 1;
            }
            break;
        case MYSQL_DUMP:
            encoding = copyStmt.getEncoding();
            if (encoding == null)
                encoding = server.getMessenger().getEncoding();
            break;
        default:
            throw new UnsupportedSQLException("COPY FORMAT " + format);
        }
        commitFrequency = copyStmt.getCommitFrequency();
        if (commitFrequency == 0) {
            commitFrequency = server.getTransactionPeriodicallyCommit() == ServerTransaction.PeriodicallyCommit.OFF ?
                    ExternalDataService.COMMIT_FREQUENCY_NEVER :
                    ExternalDataService.COMMIT_FREQUENCY_PERIODICALLY;
        }
        maxRetries = copyStmt.getMaxRetries();
        return this;
    }

    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        context.initStore(schema);
        PostgresServerSession server = context.getServer();
        Session session = server.getSession();
        ExternalDataService externalData = server.getExternalDataService();
        InputStream istr;
        long nrows = 0;
        if (fromFile != null)
            istr = new FileInputStream(fromFile);
        else
            // Always use a stream: we align records and messages, but
            // this is not a requirement on the client.
            istr = new PostgresCopyInputStream(server.getMessenger(), 
                                               toColumns.size());
        try {
            preExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
            switch (format) {
            case CSV:
                nrows = externalData.loadTableFromCsv(session, istr, csvFormat, skipRows,
                                                      toTable, toColumns,
                                                      commitFrequency, maxRetries,
                                                      context);
                break;
            case MYSQL_DUMP:
                nrows = externalData.loadTableFromMysqlDump(session, istr, encoding,
                                                            toTable, toColumns,
                                                            commitFrequency, maxRetries,
                                                            context);
                break;
            }
        }
        finally {
            postExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
            istr.close();
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
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.NONE;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

}
