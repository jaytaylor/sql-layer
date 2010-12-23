package com.akiban.ais.util;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;

public class DDLGenerator
{
    public List<String> createAllGroupTables(AkibaInformationSchema ais)
    {
        List<String> ddl = new ArrayList<String>();
        for (GroupTable groupTable : ais.getGroupTables().values()) {
            String createTable = createTable(groupTable);
            ddl.add(createTable);
        }
        return ddl;
    }

    public List<String> dropAllGroupTables(AkibaInformationSchema ais)
    {
        List<String> ddl = new ArrayList<String>();
        for (GroupTable groupTable : ais.getGroupTables().values()) {
            String dropTable = dropTable(groupTable);
            ddl.add(dropTable);
        }
        return ddl;
    }

    public String createTable(Table table)
    {
        // columns
        List<String> columnDeclarations = new ArrayList<String>();
        for (Column column : table.getColumns()) {
            columnDeclarations.add(declaration(column));
        } // indexes
        List<String> indexDeclarations = new ArrayList<String>();
        for (Index index : table.getIndexes()) {
            indexDeclarations.add(declaration(index));
        }
        // generate ddl
        return String.format(CREATE_TABLE_TEMPLATE,
                             quote(table.getName().getSchemaName()),
                             quote(table.getName().getTableName()),
                             commaSeparated(columnDeclarations),
                             indexDeclarations.isEmpty() ? "" : ", ",
                             commaSeparated(indexDeclarations));
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
        Type type = column.getType();
        final String typeName;
        final boolean typeIsUnsigned;
        if (type.name().endsWith(" unsigned")) {
            int spaceIndex = type.name().indexOf(' ');
            typeName = type.name().substring(0, spaceIndex);
            typeIsUnsigned = true;
        }
        else {
            typeName = type.name();
            typeIsUnsigned = false;
        }

        declaration.append(typeName);
        if (type.nTypeParameters() >= 1) {
            declaration.append('(');
            declaration.append(column.getTypeParameter1());
            if (type.nTypeParameters() >= 2) {
                declaration.append(", ");
                declaration.append(column.getTypeParameter2());
            }
            declaration.append(')');
        }
        if (typeIsUnsigned) {
            declaration.append(" unsigned");
        }
        return declaration.toString();
    }

    private String declaration(Index index)
    {
        List<String> indexColumnDeclarations = new ArrayList<String>();
        for (IndexColumn indexColumn : index.getColumns()) {
            indexColumnDeclarations.add(declaration(indexColumn));
        }
        return String.format(INDEX_TEMPLATE,
                             index.getConstraint(),
                             index.getIndexName().getName(),
                             commaSeparated(indexColumnDeclarations));
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
    // - index declarations
    private static final String CREATE_TABLE_TEMPLATE = "create table %s.%s(%s %s %s) engine=akibandb";
    // index declaration in create table statement. Template arguments:
    // - constraint (primary key, key, or unique)
    // - index name
    // - index column declarations (each column may have a 'desc' specifier) 
    private static final String INDEX_TEMPLATE = "%s %s(%s)";
    // drop table. Template arguments:
    // - schema name
    // - table name
    private static final String DROP_TABLE_TEMPLATE = "drop table if exists %s.%s";
}
