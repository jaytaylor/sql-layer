package com.akiban.cserver.api.common;

import com.akiban.ais.model.*;
import com.akiban.cserver.api.dml.NoSuchTableException;
import com.akiban.cserver.manage.SchemaMXBean;
import com.akiban.cserver.manage.SchemaManager;

import java.util.Collection;

public final class IdResolver {
    final SchemaManager schemaManager;

    public IdResolver(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    int tableId(TableName tableName) throws NoSuchTableException {
        final AkibaInformationSchema ais = schemaManager.getAisCopy();
        final Table aisTable = ais.getTable(tableName);
        if (aisTable == null) {
            throw new NoSuchTableException(tableName);
        }
        return aisTable.getTableId();
    }

    TableName tableName(int id) throws NoSuchTableException {
        AkibaInformationSchema ais = schemaManager.getAisCopy();
        Table found = tableById(ais.getUserTables().values(), id);
        if (found == null) {
            found = tableById(ais.getGroupTables().values(), id);
        }
        if (found == null) {
            throw new NoSuchTableException(id);
        }
        return found.getName();
    }

    private Table tableById(Collection<? extends Table> tables, int needle) {
        for (Table table : tables) {
            final Integer tableId = table.getTableId();
            if ( (tableId != null) && tableId == needle) {
                return table;
            }
        }
        return null;
    }
}
