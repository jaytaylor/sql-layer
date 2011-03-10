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

package com.akiban.server.itests.alter;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.common.ResolutionException;
import com.akiban.server.itests.ApiTestBase;

public class AlterTestBase extends ApiTestBase {
    public AkibanInformationSchema createAISWithTable(int tableId) throws NoSuchTableException, ResolutionException {
        TableName tname = tableName(tableId);
        String schemaName = tname.getSchemaName();
        String tableName = tname.getTableName();

        AkibanInformationSchema ais = new AkibanInformationSchema();
        UserTable.create(ais, schemaName, tableName, tableId);
        
        return ais;
    }

    public Index addIndexToAIS(AkibanInformationSchema ais, String sname, String tname, String iname,
            String[] refColumns, boolean isUnique) {
        Table table = ais.getTable(sname, tname);
        Table curTable = ddl().getAIS(session()).getTable(sname, tname);
        Index index = Index.create(ais, table, iname, -1, isUnique, isUnique ? "UNIQUE" : "KEY");

        if(refColumns != null) {
            int pos = 0;
            for (String colName : refColumns) {
                Column col = curTable.getColumn(colName);
                Column refCol = Column.create(table, col.getName(), col.getPosition(), col.getType());
                refCol.setTypeParameter1(col.getTypeParameter1());
                refCol.setTypeParameter2(col.getTypeParameter2());
                Integer indexedLen = col.getMaxStorageSize().intValue();
                index.addColumn(new IndexColumn(index, refCol, pos++, true, indexedLen));
            }
        }
        
        return index;
    }
    
    public List<Index> getAllIndexes(AkibanInformationSchema ais)
    {
        ArrayList<Index> indexes = new ArrayList<Index>();
        for(UserTable tbl : ais.getUserTables().values()) {
            indexes.addAll(tbl.getIndexes());
        }
        return indexes;
    }
}
