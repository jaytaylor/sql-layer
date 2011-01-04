package com.akiban.cserver;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.store.SchemaManager;
import com.persistit.exception.PersistitException;

public class RowDefCacheFactory
{
    public RowDefCache rowDefCache(String ddl) throws Exception
    {
        return rowDefCache(new String[]{ddl});
    }

    public RowDefCache rowDefCache(String[] ddl) throws Exception
    {
        StringBuilder buffer = new StringBuilder();
        for (String line : ddl) {
            buffer.append(line);
        }
        RowDefCache rowDefCache = new FakeRowDefCache();
        AkibaInformationSchema ais = new DDLSource().buildAISFromString(buffer.toString());
        rowDefCache.setAIS(ais);
        rowDefCache.fixUpOrdinals(null);
        return rowDefCache;
    }

    private static class FakeRowDefCache extends RowDefCache
    {
        @Override
        public void fixUpOrdinals(SchemaManager tableManager) throws PersistitException
        {
            assert tableManager == null;
            for (RowDef groupRowDef : getRowDefs()) {
                if (groupRowDef.isGroupTable()) {
                    groupRowDef.setOrdinal(0);
                    int userTableOrdinal = 1;
                    for (RowDef userRowDef : groupRowDef.getUserTableRowDefs()) {
                        userRowDef.setOrdinal(userTableOrdinal++);
                    }
                }
            }
        }
    }
}
