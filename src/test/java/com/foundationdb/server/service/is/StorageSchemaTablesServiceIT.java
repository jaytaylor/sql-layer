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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.PersistitITBase;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StorageSchemaTablesServiceIT extends PersistitITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(StorageSchemaTablesService.class, StorageSchemaTablesServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void examine() {
        AkibanInformationSchema ais = ais();
        assertEquals ("Table count", 11, StorageSchemaTablesServiceImpl.createTablesToRegister(ddl().getTypesTranslator()).getTables().size());
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
}
