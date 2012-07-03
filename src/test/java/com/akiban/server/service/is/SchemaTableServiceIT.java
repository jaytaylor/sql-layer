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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.service.servicemanager.GuicedServiceManager;

import com.akiban.server.test.it.ITBase;

import com.akiban.server.service.config.Property;
import java.util.Collection;

public final class SchemaTableServiceIT extends ITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bind(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class)
                .bind(StorageSchemaTablesService.class, StorageSchemaTablesServiceImpl.class)
                .overrideRequires(getClass().getResource("SchemaTableService-requires.yaml"));
    }

    @Override
    protected Collection<Property> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    private AkibanInformationSchema ais;
    
    @Before
    public void getISTables () {
        ais = serviceManager().getSchemaManager().getAis(session());
    }
    
    @Test
    public void baseInfoExamine() {
        assertEquals(ais.getUserTables().size(), 22);
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.SCHEMATA));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.TABLES));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.COLUMNS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.TABLE_CONSTRAINTS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.REFERENTIAL_CONSTRAINTS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.GROUPING_CONSTRAINTS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.KEY_COLUMN_USAGE));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.INDEXES));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.INDEX_COLUMNS));
    }
    
    @Test
    public void storageExamine() {
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_ALERTS_SUMMARY));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_BUFFER_POOLS));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_CHECKPOINT_SUMMARY));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_CLEANUP_MANAGER_SUMMARY));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_IO_METER_SUMMARY));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_IO_METERS));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_JOURNAL_MANAGER_SUMMARY));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_MANAGEMENT_SUMMARY));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_TRANSACTION_SUMMARY));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_TREES));
        assertNotNull (ais.getUserTable(StorageSchemaTablesServiceImpl.STORAGE_VOLUMES));
    }
}
