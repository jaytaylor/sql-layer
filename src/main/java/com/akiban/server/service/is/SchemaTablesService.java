
package com.akiban.server.service.is;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.memoryadapter.BasicFactoryBase;
import com.akiban.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.store.SchemaManager;

public class SchemaTablesService {
    
    static final String SCHEMA_NAME = TableName.INFORMATION_SCHEMA;
    public static final int YES_NO_MAX = 3;
    public static final int DESCRIPTOR_MAX = 32;
    public static final int IDENT_MAX = 128;
    public static final int PATH_MAX = 1024;
    protected final SchemaManager schemaManager;

    public SchemaTablesService (SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
    
    protected void attach(AkibanInformationSchema ais, boolean doRegister, TableName name, Class<? extends BasicFactoryBase> clazz) {
        UserTable table = ais.getUserTable(name);
        assert table != null;
        final BasicFactoryBase factory;
        try {
            factory = clazz.getConstructor(this.getClass(), TableName.class).newInstance(this, name);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        if(doRegister) {
            schemaManager.registerMemoryInformationSchemaTable(table, factory);
        } else {
            table.setMemoryTableFactory(factory);
        }
    }
    
    protected abstract class BaseScan implements GroupScan {
        final RowType rowType;
        int rowCounter = 0;
        
        public BaseScan (RowType rowType) {
            this.rowType = rowType;
        }
        
        @Override
        public void close() {
        }
        
    } 
}
