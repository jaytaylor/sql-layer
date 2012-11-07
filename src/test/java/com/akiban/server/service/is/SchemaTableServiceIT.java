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

import java.util.Collection;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.servicemanager.GuicedServiceManager;

import com.akiban.server.test.it.ITBase;

public final class SchemaTableServiceIT extends ITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class)
                .bindAndRequire(StorageSchemaTablesService.class, StorageSchemaTablesServiceImpl.class)
                .bindAndRequire(ServerSchemaTablesService.class, ServerSchemaTablesServiceImpl.class);
    }

    @Override
    protected Collection<Property> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    private AkibanInformationSchema ais;
    
    @Before
    public void getISTables () throws Exception {
        ais = transactionally(new Callable<AkibanInformationSchema>() {
            @Override
            public AkibanInformationSchema call() {
                return serviceManager().getSchemaManager().getAis(session());
            }
        });
    }
    
    @Test
    public void baseInfoExamine() {
        assertEquals(37, ais.getUserTables().size());
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.SCHEMATA));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.TABLES));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.COLUMNS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.TABLE_CONSTRAINTS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.REFERENTIAL_CONSTRAINTS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.GROUPING_CONSTRAINTS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.KEY_COLUMN_USAGE));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.INDEXES));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.INDEX_COLUMNS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.SEQUENCES));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.VIEWS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.VIEW_TABLE_USAGE));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.VIEW_COLUMN_USAGE));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.ROUTINES));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.PARAMETERS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.JARS));
        assertNotNull (ais.getUserTable(BasicInfoSchemaTablesServiceImpl.ROUTINE_JAR_USAGE));
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
    
    @Test
    public void serverExamine() {
        assertNotNull (ais.getUserTable(ServerSchemaTablesServiceImpl.SERVER_INSTANCE_SUMMARY));
        assertNotNull (ais.getUserTable(ServerSchemaTablesServiceImpl.SERVER_PARAMETERS));
        assertNotNull (ais.getUserTable(ServerSchemaTablesServiceImpl.SERVER_SERVERS));
        assertNotNull (ais.getUserTable(ServerSchemaTablesServiceImpl.SERVER_SESSIONS));
        assertNotNull (ais.getUserTable(ServerSchemaTablesServiceImpl.ERROR_CODES));
    }
}
