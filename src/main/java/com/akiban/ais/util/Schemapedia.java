/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;

public class Schemapedia
{
    public static void main(String[] args) throws Exception
    {
        Schemapedia schemapedia = new Schemapedia(args);
        AkibanInformationSchema ais = schemapedia.importSchemas();
        schemapedia.analyze(ais);
    }

    public AkibanInformationSchema importSchemas() throws Exception
    {
        DB.Connection connection = db.new Connection();
        // Visit each schema
        for (final String schema : schemas(connection)) {
            if (!SCHEMAS_TO_SKIP.contains(schema)) {
                List<String> tables = tables(connection, schema);
                for (final String table : tables) {
                    aisBuilder.userTable(schema, table);
                }
            }
        }
        importColumns(connection);
        importIndexes(connection);
        importForeignKeys(connection);
        return aisBuilder.akibanInformationSchema();
    }

    private void importColumns(DB.Connection connection)
        throws Exception
    {
        connection.new Query("select table_schema, " +
                             "       table_name, " +
                             "       column_name, " +
                             "       ordinal_position, " +
                             "       data_type, " +
                             "       character_maximum_length, " +
                             "       numeric_precision, " +
                             "       numeric_scale, " +
                             "       is_nullable, " +
                             "       extra " +
                             "from information_schema.columns")
        {
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                int c = 0;
                String schema = resultSet.getString(++c);
                if (!SCHEMAS_TO_SKIP.contains(schema)) {
                    String table = resultSet.getString(++c);
                    String column = resultSet.getString(++c);
                    int position = resultSet.getInt(++c);
                    String type = resultSet.getString(++c);
                    long stringSize = resultSet.getLong(++c);
                    boolean haveStringSize = !resultSet.wasNull();
                    long precision = resultSet.getLong(++c);
                    long scale = resultSet.getLong(++c);
                    boolean nullable = resultSet.getBoolean(++c);
                    String extra = resultSet.getString(++c);
                    boolean autoIncrement = extra.indexOf("auto_increment") >= 0;
                    aisBuilder.column(schema,
                                      table,
                                      column,
                                      position,
                                      type,
                                      haveStringSize ? stringSize : precision,
                                      haveStringSize ? 0L : scale,
                                      nullable,
                                      autoIncrement,
                                      null,
                                      null);
                }
            }
        }.execute();
    }

    private void importIndexes(DB.Connection connection)
        throws Exception
    {
        connection.new Query("select table_schema, " +
                             "       table_name, " +
                             "       constraint_name, " +
                             "       constraint_type " +
                             "from information_schema.table_constraints")
        {
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                int c = 0;
                String schema = resultSet.getString(++c);
                if (!SCHEMAS_TO_SKIP.contains(schema)) {
                    String table = resultSet.getString(++c);
                    String constraintName = resultSet.getString(++c);
                    String constraintType = resultSet.getString(++c);
                    boolean unique = constraintType.equals("PRIMARY") || constraintType.equals("UNIQUE");
                    aisBuilder.index(schema, table, constraintName, unique, constraintType);
                }
            }
        }.execute();
        connection.new Query("select table_schema, " +
                             "       table_name," +
                             "       constraint_name," +
                             "       column_name," +
                             "       ordinal_position " +
                             "from information_schema.key_column_usage")
        {
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                int c = 0;
                String schema = resultSet.getString(++c);
                if (!SCHEMAS_TO_SKIP.contains(schema)) {
                    String table = resultSet.getString(++c);
                    String constraintName = resultSet.getString(++c);
                    String columnName = resultSet.getString(++c);
                    int position = resultSet.getInt(++c);
                    boolean ascending = true; // not accurate, but we don't care here
                    Integer indexedLength = null; // not accurate, but we don't care here
                    aisBuilder.indexColumn(schema, table, constraintName, columnName, position, ascending, indexedLength);
                }
            }
        }.execute();
    }

    private void importForeignKeys(DB.Connection connection)
        throws Exception
    {
        connection.new Query("select constraint_schema, " +
                             "       constraint_name, " +
                             "       table_name, " +
                             "       unique_constraint_schema, " +
                             "       unique_constraint_name, " +
                             "       referenced_table_name " +
                             "from information_schema.referential_constraints")
        {
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                int c = 0;
                String childSchema = resultSet.getString(++c);
                if (!SCHEMAS_TO_SKIP.contains(childSchema)) {
                    String childConstraint = resultSet.getString(++c);
                    String childTable = resultSet.getString(++c);
                    String parentSchema = resultSet.getString(++c);
                    String parentConstraint = resultSet.getString(++c);
                    String parentTable = resultSet.getString(++c);
                    aisBuilder.joinTables(String.format("%s.%s", childSchema, childConstraint),
                                          parentSchema,
                                          parentTable,
                                          childSchema,
                                          childTable);
                }
            }
        }.execute();
    }

    public Schemapedia(String[] args)
        throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException
    {
        int a = 0;
        String host = args[a++];
        int port = Integer.valueOf(args[a++]);
        String user = args[a++];
        String password = args[a++];
        String analyzerClassName = args[a++];
        db = new DB(host, port, user, password);
        analyzer = (Analyzer) Class.forName(analyzerClassName).newInstance();
        this.aisBuilder = new AISBuilder();
    }

    public void analyze(AkibanInformationSchema ais)
    {
        analyzer.analyze(ais);
    }

    private List<String> schemas(DB.Connection connection) throws Exception
    {
        final List<String> schemas = new ArrayList<String>();
        connection.new Query("show databases")
        {
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                schemas.add(resultSet.getString(1));
            }
        }.execute();
        return schemas;
    }

    private List<String> tables(DB.Connection connection, String schema) throws Exception
    {
        final List<String> tables = new ArrayList<String>();
        connection.new DDL(String.format("use %s", schema)).execute();
        connection.new Query("show tables")
        {
            protected void handleRow(ResultSet resultSet) throws Exception
            {
                tables.add(resultSet.getString(1));
            }
        }.execute();
        return tables;
    }

    private static final Set<String> SCHEMAS_TO_SKIP =
        new HashSet<String>(Arrays.asList("mysql", "information_schema"));

    private final DB db;
    private final AISBuilder aisBuilder;
    private final Analyzer analyzer;

    // Analyzer

    public interface Analyzer
    {
        void analyze(AkibanInformationSchema ais);
    }
}
