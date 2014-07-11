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

package com.foundationdb.ais.util;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Schema;

public class DDLGenerator
{
    private final String useSchemaName;
    private final String useTableName;


    public DDLGenerator() {
        this(null,null);
    }

    public DDLGenerator(final String schemaName, final String tableName) {
        this.useSchemaName = schemaName;
        this.useTableName = tableName;
    }

    public String createTable(Table table)
    {
        // columns
        List<String> columnDeclarations = new ArrayList<>();
        for (Column column : table.getColumns()) {
            columnDeclarations.add(declaration(column));
        } 
        // indexes
        String pkeyDecl = null;
        List<String> indexDecls = new ArrayList<>();
        List<String> fkeyDecls = new ArrayList<>();
        Schema schema = table.getAIS().getSchema(table.getName().getSchemaName());
        for (TableIndex index : table.getIndexes()) {
            String decl = declaration(index, schema);
            if(index.isPrimaryKey())  {
                pkeyDecl = decl;
            }
            else if(index.isConnectedToFK(schema)) {
                fkeyDecls.add(decl);
            }
            else {
                indexDecls.add(decl);
            }
        }
        // generate ddl
        final TableName tname = table.getName();
        final String schemaName = useSchemaName != null ? useSchemaName : tname.getSchemaName();
        final String tableName = useTableName != null ? useTableName : tname.getTableName();
        return String.format(CREATE_TABLE_TEMPLATE,
                             quote(schemaName),
                             quote(tableName),
                             commaSeparated(columnDeclarations),
                             pkeyDecl == null ? "" : ", " + pkeyDecl,
                             indexDecls.isEmpty() ? "" : ", " + commaSeparated(indexDecls),
                             fkeyDecls.isEmpty() ? "" : ", " + commaSeparated(fkeyDecls),
                             tableOptions(table));
    }

    public String dropTable(Table table)
    {
        return String.format(DROP_TABLE_TEMPLATE,
                             quote(table.getName().getSchemaName()),
                             quote(table.getName().getTableName()));
    }

    private String declaration(Column column)
    {
        StringBuilder declaration = new StringBuilder();
        declaration.append(quote(column.getName()));
        declaration.append(' ');
        String typeName = column.getType().typeClass()
            .name().unqualifiedName().toLowerCase();
        boolean typeIsUnsigned = false;
        if (typeName.endsWith(" unsigned")) {
            int spaceIndex = typeName.indexOf(' ');
            typeName = typeName.substring(0, spaceIndex);
            typeIsUnsigned = true;
        }
        declaration.append(typeName);
        if (column.getTypeParameter1() != null) {
            declaration.append('(');
            declaration.append(column.getTypeParameter1());
            if (column.getTypeParameter2() != null) {
                declaration.append(", ");
                declaration.append(column.getTypeParameter2());
            }
            declaration.append(')');
        }
        if (typeIsUnsigned) {
            declaration.append(" unsigned");
        }
        if (column.hasCharsetAndCollation()) {
            final String charset = column.getCharsetName();
            final String collation = column.getCollationName();
            final String tableCharset = column.getTable().getDefaultedCharsetName();
            final String tableCollation = column.getTable().getDefaultedCollationName();
            if (charset != null &&
                charset.equals(tableCharset) == false) {
                declaration.append(" CHARACTER SET ");
                declaration.append(charset);
            }
            if (collation != null &&
                collation.equals(tableCollation) == false) {
                declaration.append(" COLLATE ");
                declaration.append(collation);
            }
        }
        
        if (column.getDefaultValue() != null) {
            declaration.append(" DEFAULT '");
            declaration.append(column.getDefaultValue());
            declaration.append("'");
        }
        if (column.getNullable() == false) {
            declaration.append(" NOT");
        }
        declaration.append(" NULL");
        if (column.getInitialAutoIncrementValue() != null) {
            declaration.append(" AUTO_INCREMENT");
        }
        return declaration.toString();
    }

    private String declaration(TableIndex index, Schema schema)
    {
        List<String> columnDecls = new ArrayList<>();
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            columnDecls.add(declaration(indexColumn));
        }
        
        if(index.isConnectedToFK(schema)){
            Join join = index.getTable().getParentJoin();
            
            if(join == null) {
                return new String("");
            }
            
            List<String> parentColumnDecls = new ArrayList<>();
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                parentColumnDecls.add(quote(joinColumn.getParent().getName()));
            }
            
            return String.format("CONSTRAINT %s FOREIGN KEY %s(%s) REFERENCES %s(%s)", 
                                 quote(index.getIndexName().getName()),             
                                 quote(index.getIndexName().getName()),
                                 commaSeparated(columnDecls),
                                 quote(join.getParent().getName().getTableName()),
                                 commaSeparated(parentColumnDecls));
        }
        if (index.isPrimaryKey()) {
            return String.format(INDEX_TEMPLATE, "PRIMARY KEY", "",
                    commaSeparated(columnDecls));
        } else if (index.isUnique()) {
            return String.format(INDEX_TEMPLATE, "UNIQUE",
                    " " + quote(index.getIndexName().getName()),
                    commaSeparated(columnDecls));
        } else {
            return String.format(INDEX_TEMPLATE, "KEY",
                    " " + quote(index.getIndexName().getName()),
                    commaSeparated(columnDecls));
        }
    }

    private String declaration(IndexColumn indexColumn)
    {
        StringBuilder declaration = new StringBuilder();
        declaration.append(quote(indexColumn.getColumn().getName()));
        if (!indexColumn.isAscending()) {
            declaration.append(" desc");
        }
        return declaration.toString();
    }

    private String tableOptions(Table table) {
        StringBuilder tableOptions = new StringBuilder();
        final String engine = "akibandb";
        tableOptions.append(" engine=");
        tableOptions.append(engine);
        String charset = table.getDefaultedCharsetName();
        if (charset != null) {
            tableOptions.append(" DEFAULT CHARSET=");
            tableOptions.append(charset);
        }
        String collation = table.getDefaultedCollationName();
        if (collation != null) {
            tableOptions.append(" COLLATE=");
            tableOptions.append(collation);
        }
        return tableOptions.toString();
    }

    private static String commaSeparated(List<String> list)
    {
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (String s : list) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(s);
        }
        return buffer.toString();
    }

    private static String quote(String s)
    {
        return String.format("`%s`", s);
    }

    // create table. Template arguments:
    // - schema name
    // - table name
    // - column declarations
    // - comma, if there are index declarations
    // - primary key declarations
    // - index declarations
    // - foreign key declarations
    // - table options
    private static final String CREATE_TABLE_TEMPLATE = "create table %s.%s(%s%s%s%s)%s";
    // index declaration in create table statement. Template arguments:
    // - constraint (primary key, key, or unique)
    // - index name
    // - index column declarations (each column may have a 'desc' specifier) 
    private static final String INDEX_TEMPLATE = "%s%s(%s)";
    // drop table. Template arguments:
    // - schema name
    // - table name
    private static final String DROP_TABLE_TEMPLATE = "drop table if exists %s.%s";
}
