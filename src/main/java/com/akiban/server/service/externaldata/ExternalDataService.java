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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
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

    long loadTableFromCsv(Session session, InputStream inputStream, 
                          CsvFormat format, long skipRows,
                          UserTable toTable, List<Column> toColumns,
                          long commitFrequency, QueryContext context) throws IOException;

    long loadTableFromMysqlDump(Session session, InputStream inputStream, String encoding,
                                UserTable toTable, List<Column> toColumns,
                                long commitFrequency, QueryContext context) throws IOException;
}
