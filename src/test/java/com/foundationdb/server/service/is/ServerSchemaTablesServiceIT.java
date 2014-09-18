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
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ServerSchemaTablesServiceIT extends ITBase
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(ServerSchemaTablesService.class, ServerSchemaTablesServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void examine() {
        AkibanInformationSchema ais = ais();
        assertEquals ("Table count", 12, ServerSchemaTablesServiceImpl.createTablesToRegister(ddl().getTypesTranslator()).getTables().size());
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.ERROR_CODES));
        assertNotNull (ais.getTable(ServerSchemaTablesServiceImpl.ERROR_CODE_CLASSES));
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
