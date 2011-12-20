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

package com.akiban.server.rowdata;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Table;
import com.akiban.server.MemoryOnlyTableStatusCache;
import com.akiban.server.error.PersistItErrorException;
import com.persistit.exception.PersistitInterruptedException;

import java.util.HashMap;
import java.util.Map;

public class SchemaFactory {
    public RowDefCache rowDefCache(String ddl) throws Exception {
        return rowDefCache(new String[] { ddl });
    }

    public RowDefCache rowDefCache(String[] ddl) throws Exception {
        AkibanInformationSchema ais = ais(ddl);
        return rowDefCache(ais);
    }

    public AkibanInformationSchema ais(String[] ddl) throws Exception {
        StringBuilder buffer = new StringBuilder();
        for (String line : ddl) {
            buffer.append(line);
        }
        final SchemaDefToAis toAis = new SchemaDefToAis(
                SchemaDef.parseSchema(buffer.toString()), false);
        return toAis.getAis();
    }

    public RowDefCache rowDefCache(AkibanInformationSchema ais) {
        RowDefCache rowDefCache = new FakeRowDefCache();
        try {
            rowDefCache.setAIS(ais);
        } catch(PersistitInterruptedException e) {
            throw new PersistItErrorException(e);
        }
        return rowDefCache;
    }

    private static class FakeRowDefCache extends RowDefCache {
        public FakeRowDefCache() {
            super(new MemoryOnlyTableStatusCache());
        }

        @Override
        protected Map<Table,Integer> fixUpOrdinals() throws PersistitInterruptedException {
            Map<Table,Integer> ordinalMap = new HashMap<Table,Integer>();
            for (RowDef groupRowDef : getRowDefs()) {
                if (groupRowDef.isGroupTable()) {
                    ordinalMap.put(groupRowDef.table(), 0);
                    int userTableOrdinal = 1;
                    for (RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
                        int ordinal = userTableOrdinal++;
                        tableStatusCache.setOrdinal(userRowDef.getRowDefId(), ordinal);
                        userRowDef.setOrdinalCache(ordinal);
                        ordinalMap.put(userRowDef.table(), ordinal);
                    }
                }
            }
            return ordinalMap;
        }
    }
}
