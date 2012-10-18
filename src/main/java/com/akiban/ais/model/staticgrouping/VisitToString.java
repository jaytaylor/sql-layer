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

package com.akiban.ais.model.staticgrouping;

import java.util.List;

import com.akiban.ais.model.TableName;

final class VisitToString implements GroupingVisitor<String> {
    final static String NL = System.getProperty("line.separator");
    
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
        if (!group.getGroupName().equals(rootTable)) {
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
        builder.replace(builder.length() - NL.length(), builder.length(), ";");
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
