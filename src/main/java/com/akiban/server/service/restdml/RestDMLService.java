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

package com.akiban.server.service.restdml;

import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;
import org.codehaus.jackson.JsonNode;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface RestDMLService {
    public void getAllEntities(PrintWriter writer, TableName tableName, Integer depth);
    public void getEntities(PrintWriter writer, TableName tableName, Integer depth, String pks);
    public void insert(PrintWriter writer, TableName tableName, JsonNode node);
    public void delete(PrintWriter writer, TableName tableName, String pks);
    public void update(PrintWriter writer, TableName tableName, String values, JsonNode node);

    public void runSQL(PrintWriter writer, HttpServletRequest request, String sql, String schema) throws SQLException;
    public void runSQL(PrintWriter writer, HttpServletRequest request, List<String> sql) throws SQLException;
    public void runSQLParameter(PrintWriter writer,HttpServletRequest request, String SQL, List<String> parameters) throws SQLException;
    public void explainSQL(PrintWriter writer, HttpServletRequest request, String sql) throws IOException, SQLException;

    public void callProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                              TableName procName, Map<String,List<String>> params) throws SQLException;
    public void callProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                              TableName procName, String jsonParams) throws SQLException;

    public void fullTextSearch(PrintWriter writer, IndexName indexName, Integer depth, String query, Integer limit);
    // TODO: Temporary.
    public void refreshFullTextIndex(PrintWriter writer, IndexName indexName);

    public String ajdaxToSQL(TableName tableName, String ajdax) throws IOException;
}
