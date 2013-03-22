
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
    final static String DEFAULT_PARAM_NAME = "param";
    final static String DEFAULT_RESUlT_NAME = "result"; 
    
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
                              TableName procName, Map<String,List<String>> queryParams) throws SQLException;
    public void callProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                              TableName procName, String jsonParams) throws SQLException;

    public void fullTextSearch(PrintWriter writer, IndexName indexName, Integer depth, String query, Integer limit);
    // TODO: Temporary.
    public void refreshFullTextIndex(PrintWriter writer, IndexName indexName);

    public String ajdaxToSQL(TableName tableName, String ajdax) throws IOException;
}
