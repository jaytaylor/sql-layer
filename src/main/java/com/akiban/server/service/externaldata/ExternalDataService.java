
package com.akiban.server.service.externaldata;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.session.Session;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;

public interface ExternalDataService {
    /**
     * Dump entire group, starting at the given table's depth, in JSON format.
     *
     * @param depth How far down the branch to dump: -1 = all, 0 = just tableName, 1 = children, 2 = grand-children, ...
     * @param withTransaction If <code>true</code>, start a transaction before scanning.
     */
    void dumpAllAsJson(Session session, PrintWriter writer,
                       String schemaName, String tableName,
                       int depth, boolean withTransaction);

    /**
     * Dump selected branches, identified by a list of PRIMARY KEY files, in JSON format.
     *
     * @param keys PRIMARY KEY values, which each may be multiple columns, to dump.
     * @param depth How far down the branch to dump: -1 = all, 0 = just tableName, 1 = children, 2 = grand-children, ...
     * @param withTransaction If <code>true</code>, start a transaction before scanning.
     */
    void dumpBranchAsJson(Session session, PrintWriter writer,
                          String schemaName, String tableName, 
                          List<List<String>> keys, int depth,
                          boolean withTransaction);

    /**
     * Dump selected branches, given a generator of branch rows.
     */
    void dumpBranchAsJson(Session session, PrintWriter writer,
                          String schemaName, String tableName, 
                          Operator scan, RowType scanType, int depth,
                          boolean withTransaction);

    long loadTableFromCsv(Session session, InputStream inputStream, 
                          CsvFormat format, long skipRows,
                          UserTable toTable, List<Column> toColumns,
                          long commitFrequency, QueryContext context) throws IOException;

    long loadTableFromMysqlDump(Session session, InputStream inputStream, String encoding,
                                UserTable toTable, List<Column> toColumns,
                                long commitFrequency, QueryContext context) throws IOException;
}
