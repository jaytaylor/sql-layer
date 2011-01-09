package com.akiban.cserver.api.common;

import java.util.Collection;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;
import com.akiban.cserver.util.RowDefNotFoundException;
import com.akiban.util.ArgumentValidation;

public final class IdResolverImpl implements IdResolver {
    final SchemaManager schemaManager;
    final Store store;

    public IdResolverImpl(Store store) {
        ArgumentValidation.notNull("store", store);
        this.schemaManager = ServiceManagerImpl.get().getSchemaManager();
        this.store = store;
    }

    @Override
    public int tableId(TableName tableName) throws NoSuchTableException {
        final AkibaInformationSchema ais = schemaManager.getAis(new SessionImpl());
        final Table aisTable = ais.getTable(tableName);
        if (aisTable == null) {
            throw new NoSuchTableException(tableName);
        }
        return aisTable.getTableId();
    }

    @Override
    public TableName tableName(int id) throws NoSuchTableException {
        AkibaInformationSchema ais = schemaManager.getAis(new SessionImpl());
        Table found = tableById(ais.getUserTables().values(), id);
        if (found == null) {
            found = tableById(ais.getGroupTables().values(), id);
        }
        if (found == null) {
            throw new NoSuchTableException(id);
        }
        return found.getName();
    }

    @Override
    public RowDef getRowDef(TableId id) throws NoSuchTableException {
        final int idInt = id.getTableId(this);
        try {
            RowDef rowDef = store.getRowDefCache().getRowDef(idInt);
            assert rowDef != null;
            return rowDef;
        } catch (RowDefNotFoundException e) {
            throw new NoSuchTableException(idInt);
        }
    }

    private Table tableById(Collection<? extends Table> tables, int needle) {
        for (Table table : tables) {
            final Integer tableId = table.getTableId();
            if ((tableId != null) && tableId == needle) {
                return table;
            }
        }
        return null;
    }
}
