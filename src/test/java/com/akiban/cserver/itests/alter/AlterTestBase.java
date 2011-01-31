package com.akiban.cserver.itests.alter;

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.api.common.NoSuchTableException;
import com.akiban.cserver.api.common.ResolutionException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;

public class AlterTestBase extends ApiTestBase {
    public AkibaInformationSchema createAISWithTable(TableId id) throws NoSuchTableException, ResolutionException {
        ddl().resolveTableId(id);
        Integer tableId = id.getTableId(null);
        TableName tname = ddl().getTableName(id);
        String schemaName = tname.getSchemaName();
        String tableName = tname.getTableName();

        AkibaInformationSchema ais = new AkibaInformationSchema();
        UserTable.create(ais, schemaName, tableName, tableId);
        
        return ais;
    }

    public Index addIndexToAIS(AkibaInformationSchema ais, String sname, String tname, String iname, 
            String[] refColumns, boolean isUnique) {
        Table table = ais.getTable(sname, tname);
        Table curTable = ddl().getAIS(session).getTable(sname, tname);
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
    
    public List<Index> getAllIndexes(AkibaInformationSchema ais)
    {
        ArrayList<Index> indexes = new ArrayList<Index>();
        for(UserTable tbl : ais.getUserTables().values()) {
            indexes.addAll(tbl.getIndexes());
        }
        return indexes;
    }
}
