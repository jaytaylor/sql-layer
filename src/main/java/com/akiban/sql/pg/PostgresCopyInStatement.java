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

import com.akiban.sql.parser.ColumnReference;
import com.akiban.sql.parser.CopyStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.ResultColumn;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.server.ServerTransaction;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.service.externaldata.CsvFormat;
import com.akiban.server.service.externaldata.ExternalDataService;
import com.akiban.server.service.session.Session;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/** COPY ... FROM */
public class PostgresCopyInStatement extends PostgresBaseStatement
{
    private UserTable toTable;
    private List<Column> toColumns;
    private File fromFile;
    private CopyStatementNode.Format format;
    private CsvFormat csvFormat;
    private long skipRows;
    private long commitFrequency;

    private static final Logger logger = LoggerFactory.getLogger(PostgresCopyInStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresCopyInStatement: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresCopyInStatement: acquire shared lock");

    protected PostgresCopyInStatement() {
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
        toTable = server.getAIS().getUserTable(schemaName, tableName);
        if (toTable == null)
            throw new NoSuchTableException(schemaName, tableName, 
                                           copyStmt.getTableName());
        if (copyStmt.getColumnList() == null)
            toColumns = toTable.getColumns();
        else {
            toColumns = new ArrayList<Column>(copyStmt.getColumnList().size());
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
        default:
            throw new UnsupportedSQLException("COPY FORMAT " + format);
        }
        commitFrequency = copyStmt.getCommitFrequency();
        if (commitFrequency == 0)
            commitFrequency = Long.MAX_VALUE; // Or let it choose?
        return this;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        Session session = server.getSession();
        ExternalDataService externalData = server.getExternalDataService();
        InputStream istr;
        long nrows = 0;
        boolean lockSuccess = false;
        if (fromFile != null)
            istr = new FileInputStream(fromFile);
        else
            // Always use a stream: we align records and messages, but
            // this is not a requirement on the client.
            istr = new PostgresCopyInputStream(server.getMessenger(), 
                                               toColumns.size());
        try {
            lock(context, DXLFunction.UNSPECIFIED_DML_WRITE);
            lockSuccess = true;
            switch (format) {
            case CSV:
                nrows = externalData.loadTableFromCsv(session, istr, csvFormat, skipRows,
                                                      toTable, toColumns,
                                                      commitFrequency, context);
                break;
            }
        }
        finally {
            unlock(context, DXLFunction.UNSPECIFIED_DML_WRITE, lockSuccess);
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
    public void sendDescription(PostgresQueryContext context, boolean always) 
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
    protected InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

}
