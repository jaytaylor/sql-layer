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

package com.akiban.server.types3.playground.exprfinder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class MySqlAdapter implements DbAdapter {

    @Override
    public void createTable(List<Declaration> declarations) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(sourceTable).append("(");
        for (Iterator<Declaration> iterator = declarations.iterator(); iterator.hasNext(); ) {
            Declaration declaration = iterator.next();
            sb.append(declaration.columnName()).append(" ").append(declaration.columnDeclaration());

            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(")");
        query(sb.toString());
    }

    @Override
    public Map<String, String> getDefinitions() throws SQLException {
        return getTableDefinitions(sourceTable);
    }

    @Override
    public String getResultDefinition(String query) throws SQLException {
        final String fullQuery = "create table " + destTable + " as select " + query + " from " + sourceTable;
        query(fullQuery);
        Map<String,String> defs = getTableDefinitions(destTable);
        if (defs.size() != 1)
            throw new RuntimeException("wrong table definitions size: " + defs);
        return defs.values().iterator().next();
    }

    @Override
    public void load(String username, String password) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(String.format("jdbc:mysql://localhost/?user=%s&password=%s",
                    username, password));
        } catch (Exception e) {
            throw new RuntimeException("couldn't load MySQL driver", e);
        }
    }

    @Override
    public void init() throws SQLException {
        query("drop schema if exists " + schema);
        query("create schema " + schema);
        query("use " + schema);
    }

    @Override
    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<String, String> getTableDefinitions(String table) throws SQLException {
        return query("describe " + table, new SqlLambda<Map<String, String>>() {
            @Override
            public Map<String, String> apply(ResultSet input) throws SQLException {
                Map<String, String> results = new LinkedHashMap<String, String>();
                while (input.next()) {
                    String fieldName = input.getString(1);
                    String fieldDef = input.getString(2);
                    String old = results.put(fieldName, fieldDef);
                    assert old == null : old;
                }
                return results;
            }
        });
    }

    private void query(String query) throws SQLException {
        query(query, new SqlLambda<Void>() {
            @Override
            public Void apply(ResultSet input) {
                return null;
            }
        });
    }

    private <T> T query(String query, SqlLambda<T> function) throws SQLException {
        Statement s = conn.createStatement();
        try {
            boolean hasResultSet = s.execute(query);
            if (!hasResultSet)
                return null;
            T result = function.apply(s.getResultSet());
            assert ! s.getMoreResults();
            return result;
        } finally {
            s.close();
        }
    }

    public MySqlAdapter(String schema, String sourceTable, String destTable) {
        this.schema = schema;
        this.sourceTable = sourceTable;
        this.destTable = destTable;
    }

    private Connection conn;
    private final String schema;
    private final String sourceTable;
    private final String destTable;

    private interface SqlLambda<T> {
        T apply(ResultSet resultSet) throws SQLException;
    }
}
