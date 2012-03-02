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
