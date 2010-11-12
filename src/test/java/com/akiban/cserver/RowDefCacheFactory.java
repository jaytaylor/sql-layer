package com.akiban.cserver;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;

public class RowDefCacheFactory
{
    public RowDefCache rowDefCache(String[] ddl) throws Exception
    {
        StringBuilder buffer = new StringBuilder();
        for (String line : ddl) {
            buffer.append(line);
        }
        RowDefCache rowDefCache = new RowDefCache();
        AkibaInformationSchema ais = new DDLSource().buildAISFromString(buffer.toString());
        rowDefCache.setAIS(ais);
        return rowDefCache;
    }
}
