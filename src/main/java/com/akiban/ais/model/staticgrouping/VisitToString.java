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

package com.akiban.ais.model.staticgrouping;

import java.util.List;

import com.akiban.ais.model.TableName;

final class VisitToString implements GroupingVisitor<String> {
    final static String NL = "\n";
    
    public static VisitToString getInstance() {
        return new VisitToString(); // singleton would have concurrency issues
    }

    private VisitToString() {

    }

    private StringBuilder builder;
    private String visitorDefaultSchema;
    private int depth;
    boolean firstGroup;

    @Override
    public void start(String defaultSchema) {
        this.visitorDefaultSchema = defaultSchema;
        builder = new StringBuilder("groupschema ").append(defaultSchema).append(NL).append(NL);
        depth = 0;
        firstGroup = true;
    }

    @Override
    public void visitGroup(Group group, TableName rootTable) {
        if (!firstGroup) {
            builder.append(NL).append(NL);
        }
        else {
            firstGroup = false;
        }
        builder.append("CREATE GROUP");
        if (!group.getGroupName().equals(rootTable.getTableName())) {
            builder.append(' ').append(group.getGroupName());
        }
        builder.append(NL).append("ROOT TABLE ").append(rootTable.escaped(visitorDefaultSchema));
        builder.append(',').append(NL); // will get lopped off when we visit the first child
    }

    @Override
    public void finishGroup() {
        assert depth == 0 : "unexpected depth: " + depth;
        deleteExtraComma();
        // Turn the last newline into a semicolon
        builder.setCharAt(builder.length() - 1, ';');
    }

    @Override
    public void visitChild(TableName parentName, List<String> parentColumns, TableName childName, List<String> childColumns) {
        indent(builder, depth).append("TABLE ").append(childName.escaped(visitorDefaultSchema)).append(" (");
        escape(childColumns, builder);
        builder.append(") REFERENCES ").append(parentName.escaped(visitorDefaultSchema)).append(" (");
        escape(parentColumns, builder);
        builder.append("),").append(NL);
    }

    @Override
    public boolean startVisitingChildren() {
        deleteExtraComma();
        indent(builder, depth++).append("(").append(NL);
        return true;
    }

    @Override
    public void finishVisitingChildren() {
        deleteExtraComma();
        indent(builder, --depth).append("),").append(NL);
    }

    @Override
    public String end() {
        return builder.toString();
    }

    void deleteExtraComma() {
        // we have a "),\n" and from visiting the last child; we want to delete the comma
        builder.deleteCharAt(builder.length() - NL.length() - 1);
    }

    private static StringBuilder indent(StringBuilder builder, int indent) {
        assert indent >= 0 : "indent is below 0: " + indent;
        while ( indent-- > 0) {
            builder.append("    ");
        }
        return builder;
    }

    private static String escape(List<String> columns, StringBuilder builder) {
        for (String col : columns) {
            if (col.indexOf('`') >= 0) {
                builder.append('`').append(col.replace("`", "``")).append('`');
            }
            else {
                builder.append(col);
            }
            builder.append(", ");
        }
        if (builder.length() > 1) {
            builder.setLength(builder.length() - 2);
        }
        return builder.toString();
    }
}
