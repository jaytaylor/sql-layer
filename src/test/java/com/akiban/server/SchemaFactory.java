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

package com.akiban.server;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.store.SchemaManager;
import com.persistit.exception.PersistitException;

public class SchemaFactory
{
    public RowDefCache rowDefCache(String ddl) throws Exception
    {
        return rowDefCache(new String[]{ddl});
    }

    public RowDefCache rowDefCache(String[] ddl) throws Exception
    {
        AkibanInformationSchema ais = ais(ddl);
        RowDefCache rowDefCache = new FakeRowDefCache();
        rowDefCache.setAIS(ais);
        rowDefCache.fixUpOrdinals(0, null);
        return rowDefCache;
    }

    public AkibanInformationSchema ais(String[] ddl) throws Exception
    {
        StringBuilder buffer = new StringBuilder();
        for (String line : ddl) {
            buffer.append(line);
        }
        final SchemaDefToAis toAis = new SchemaDefToAis(SchemaDef.parseSchema(buffer.toString()), false);
        return toAis.getAis();
    }

    private static class FakeRowDefCache extends RowDefCache
    {
        @Override
        public void fixUpOrdinals(final long timestamp, SchemaManager schemaManager) throws PersistitException
        {
            assert schemaManager == null;
            for (RowDef groupRowDef : getRowDefs()) {
                if (groupRowDef.isGroupTable()) {
                    groupRowDef.setOrdinal(0, 0);
                    int userTableOrdinal = 1;
                    for (RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
                        userRowDef.setOrdinal(0, userTableOrdinal++);
                    }
                }
            }
        }
    }
}
