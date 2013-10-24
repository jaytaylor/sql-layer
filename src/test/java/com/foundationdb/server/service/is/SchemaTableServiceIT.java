/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.is;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;

import com.foundationdb.server.test.it.ITBase;

public final class SchemaTableServiceIT extends ITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class)
                .bindAndRequire(StorageSchemaTablesService.class, StorageSchemaTablesServiceImpl.class)
                .bindAndRequire(ServerSchemaTablesService.class, ServerSchemaTablesServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
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
        assertEquals ("Table count", 22, BasicInfoSchemaTablesServiceImpl.createTablesToRegister().getTables().size());
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.SCHEMATA));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.TABLES));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.COLUMNS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.TABLE_CONSTRAINTS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.REFERENTIAL_CONSTRAINTS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.GROUPING_CONSTRAINTS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.KEY_COLUMN_USAGE));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.INDEXES));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.INDEX_COLUMNS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.SEQUENCES));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.VIEWS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.VIEW_TABLE_USAGE));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.VIEW_COLUMN_USAGE));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.ROUTINES));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.PARAMETERS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.JARS));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.ROUTINE_JAR_USAGE));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.TYPES));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.TYPE_ATTRIBUTES));
        assertNotNull (ais.getTable(BasicInfoSchemaTablesServiceImpl.TYPE_BUNDLES));
    }
    
    @Test
    public void storageExamine() {
        assertEquals ("Table count", 11, StorageSchemaTablesServiceImpl.createTablesToRegister().getTables().size());
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_ALERTS_SUMMARY));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_BUFFER_POOLS));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_CHECKPOINT_SUMMARY));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_CLEANUP_MANAGER_SUMMARY));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_IO_METER_SUMMARY));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_IO_METERS));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_JOURNAL_MANAGER_SUMMARY));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_MANAGEMENT_SUMMARY));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_TRANSACTION_SUMMARY));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_TREES));
        assertNotNull (ais.getTable(StorageSchemaTablesServiceImpl.STORAGE_VOLUMES));
    }
    
    @Test
    public void serverExamine() {
        assertEquals ("Table count", 11, ServerSchemaTablesServiceImpl.createTablesToRegister().getTables().size());
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.ERROR_CODES));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_INSTANCE_SUMMARY));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_SERVERS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_SESSIONS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_PARAMETERS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_MEMORY_POOLS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_GARBAGE_COLLECTORS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_TAPS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_PREPARED_STATEMENTS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_CURSORS));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.SERVER_USERS));
    }
}
