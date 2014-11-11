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

package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.FormatOptions;

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
                       int depth, boolean withTransaction, FormatOptions options);

    /**
     * Dump selected branches, identified by a list of PRIMARY KEY files, in JSON format.
     *
     * @param keys PRIMARY KEY values, which each may be multiple columns, to dump.
     * @param depth How far down the branch to dump: -1 = all, 0 = just tableName, 1 = children, 2 = grand-children, ...
     * @param withTransaction If <code>true</code>, start a transaction before scanning.
     */
    void dumpBranchAsJson(Session session, PrintWriter writer,
                          String schemaName, String tableName, 
                          List<List<Object>> keys, int depth,
                          boolean withTransaction, FormatOptions options);

    /**
     * Dump selected branches, given a generator of branch rows.
     */
    void dumpBranchAsJson(Session session, PrintWriter writer,
                          String schemaName, String tableName, 
                          Operator scan, RowType scanType, int depth,
                          boolean withTransaction, FormatOptions options);

    static final long COMMIT_FREQUENCY_NEVER = -1;
    static final long COMMIT_FREQUENCY_PERIODICALLY = -2;

    long loadTableFromCsv(Session session, InputStream inputStream, 
                          CsvFormat format, long skipRows,
                          Table toTable, List<Column> toColumns,
                          long commitFrequency, int maxRetries,
                          QueryContext context) throws IOException;

    long loadTableFromMysqlDump(Session session, InputStream inputStream, String encoding,
                                Table toTable, List<Column> toColumns,
                                long commitFrequency, int maxRetries,
                                QueryContext context) throws IOException;
    
}
