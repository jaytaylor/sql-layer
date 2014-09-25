/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.Test;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.service.TypesRegistrySchemaTablesService;
import com.foundationdb.server.types.service.TypesRegistrySchemaTablesServiceImpl;

public class TypesRegistrySchemaTablesServiceIT extends ITBase {
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(TypesRegistrySchemaTablesService.class, TypesRegistrySchemaTablesServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void examine() {
        AkibanInformationSchema ais = ais();
        assertNotNull (ais.getTable(TypesRegistrySchemaTablesServiceImpl.AK_OVERLOADS));
        assertNotNull (ais.getTable(TypesRegistrySchemaTablesServiceImpl.AK_CASTS));
    }

}
