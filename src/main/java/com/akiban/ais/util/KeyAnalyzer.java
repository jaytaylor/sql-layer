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

import java.util.Map;
import java.util.TreeMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;

public class KeyAnalyzer implements Schemapedia.Analyzer
{
    @Override
    public void analyze(AkibanInformationSchema ais)
    {
        for (UserTable table : ais.getUserTables().values()) {
            String schema = table.getName().getSchemaName();
            SchemaSummary schemaSummary = schemaSummaries.get(schema);
            if (schemaSummary == null) {
                schemaSummary = new SchemaSummary();
                schemaSummaries.put(schema, schemaSummary);
            }
            schemaSummary.tables++;
            if (table.getPrimaryKey() == null) {
                schemaSummary.noPrimaryKeyTables++;
            }
            schemaSummary.foreignKeyConstraints += table.getCandidateChildJoins().size();
            for (Index index : table.getIndexes()) {
                if (index.isUnique()) {
                    schemaSummary.uniqueConstraints++;
                    boolean indexIncludesAutoIncrement = false;
                    for (IndexColumn indexColumn : index.getColumns()) {
                        if (indexColumn.getColumn() == table.getAutoIncrementColumn()) {
                            indexIncludesAutoIncrement = true;
                        }
                    }
                    if (indexIncludesAutoIncrement) {
                        schemaSummary.autoIncrementUniqueConstraints++;
                    }
                }
            }
        }
        for (Map.Entry<String, SchemaSummary> entry : schemaSummaries.entrySet()) {
            String schema = entry.getKey();
            SchemaSummary schemaSummary = entry.getValue();
            print("%s:", schema);
            print("    tables: %s", schemaSummary.tables);
            print("    PK and uniqueness constraints: %s", schemaSummary.uniqueConstraints);
            print("    PK and uniqueness constraints with autoinc: %s", schemaSummary.autoIncrementUniqueConstraints);
            print("    tables with no PK: %s", schemaSummary.noPrimaryKeyTables);
            print("    FK constraints: %s", schemaSummary.foreignKeyConstraints);
        }
    }

    private void print(String template, Object... args)
    {
        System.out.println(String.format(template, args));
    }

    private final TreeMap<String, SchemaSummary> schemaSummaries = new TreeMap<String, SchemaSummary>();

    private static class SchemaSummary
    {
        int tables = 0;
        int noPrimaryKeyTables = 0;
        int foreignKeyConstraints = 0;
        int uniqueConstraints = 0;
        int autoIncrementUniqueConstraints = 0;
    }
}
