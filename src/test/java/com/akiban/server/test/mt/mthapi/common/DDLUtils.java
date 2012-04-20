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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.server.test.mt.mthapi.base.sais.SaisFK;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;

import java.util.Iterator;

public final class DDLUtils {

    public static String buildDDL(SaisTable table) {
        return buildDDL(table, new StringBuilder());
    }

    public static String buildDDL(SaisTable table, StringBuilder builder) {
        // fields
        builder.setLength(0);
        builder.append("CREATE TABLE ").append(table.getName()).append('(');
        Iterator<String> fields = table.getFields().iterator();
        boolean first = true;
        while (fields.hasNext()) {
            String field = fields.next();
            builder.append(field).append(" int");
            if (table.getPK() != null && table.getPK().contains(field)) {
                builder.append(" not null");
                first = false;
            }
            if (fields.hasNext()) {
                builder.append(',');
            }
        }

        // PK
        if (table.getPK() != null) {
            builder.append(", PRIMARY KEY ");
            cols(table.getPK().iterator(), builder);
        }

        // AkibanFK: CONSTRAINT __akiban_fk_FOO FOREIGN KEY __akiban_fk_FOO(pid1,pid2) REFERENCES parent(id1,id2)
        SaisFK parentFK = table.getParentFK();
        if (parentFK != null) {
            builder.append(", GROUPING FOREIGN KEY");
            cols(parentFK.getChildCols(), builder).append(" REFERENCES ").append(parentFK.getParent().getName());
            cols(parentFK.getParentCols(), builder);
        }

        return builder.append(')').toString();
    }

    private static StringBuilder cols(Iterator<String> columns, StringBuilder builder) {
        builder.append('(');
        while (columns.hasNext()) {
            builder.append(columns.next());
            if (columns.hasNext()) {
                builder.append(',');
            }
        }
        builder.append(')');
        return builder;
    }

    private static StringBuilder akibanFK(SaisTable child, StringBuilder builder) {
        return builder.append("`__akiban_fk_").append(child.getName()).append('`');
    }
}
