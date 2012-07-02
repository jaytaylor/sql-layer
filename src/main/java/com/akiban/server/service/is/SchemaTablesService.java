/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
    public static final int IDENT_MAX = 128;
    public static final int YES_NO_MAX = 3;
    public static final int PATH_MAX = 1024;
    public static final int DESCRIPTOR_MAX = 32;
    protected final SchemaManager schemaManager;

    public SchemaTablesService (SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
    
    protected void attach(AkibanInformationSchema ais, boolean doRegister, TableName name, Class<? extends BasicFactoryBase> clazz) {
        UserTable table = ais.getUserTable(name);
        assert table != null;
        final BasicFactoryBase factory;
        try {
            factory = clazz.getConstructor(getClass(), TableName.class).newInstance(this, name);
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
