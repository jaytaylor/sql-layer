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

package com.foundationdb.rest.dml;

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.TableName;
import com.fasterxml.jackson.databind.JsonNode;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.foundationdb.server.service.session.Session;

public interface RestDMLService {
    public void getAllEntities(PrintWriter writer, TableName tableName, Integer depth);
    public void getEntities(PrintWriter writer, TableName tableName, Integer depth, String pks);
    public void insert(PrintWriter writer, TableName tableName, JsonNode node);
    public void delete(TableName tableName, String pks);
    public void update(PrintWriter writer, TableName tableName, String values, JsonNode node);
    public void upsert(PrintWriter writer, TableName tableName, JsonNode node);

    public void insertNoTxn(Session session, PrintWriter writer, TableName tableName, JsonNode node);
    public void updateNoTxn(Session session, PrintWriter writer, TableName tableName, String values, JsonNode node);

    public void runSQL(PrintWriter writer, HttpServletRequest request, String sql, String schema) throws SQLException;
    public void runSQL(PrintWriter writer, HttpServletRequest request, List<String> sql) throws SQLException;
    public void runSQLParameter(PrintWriter writer,HttpServletRequest request, String SQL, List<String> parameters) throws SQLException;
    public void explainSQL(PrintWriter writer, HttpServletRequest request, String sql) throws IOException, SQLException;

    public void callProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                              TableName procName, Map<String,List<String>> queryParams, String content) throws SQLException;

    public void fullTextSearch(PrintWriter writer, IndexName indexName, Integer depth, String query, Integer limit);
}
