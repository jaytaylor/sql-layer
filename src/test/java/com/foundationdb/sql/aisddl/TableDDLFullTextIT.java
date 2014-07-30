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

package com.foundationdb.sql.aisddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.junit.Test;

import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.text.FullTextIndexService;
import com.foundationdb.server.service.text.FullTextIndexServiceImpl;

public class TableDDLFullTextIT extends AISDDLITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);        
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(TableDDLIT.class);
    }

    @Test
    public void createFullTextIndexTable() throws Exception {
        String sql = "CREATE TABLE test.t17 (c1 varchar(1000), INDEX t17_ft (FULL_TEXT(c1)))";
        executeDDL(sql);
        Table table = ais().getTable("test","t17");
        assertNull (table.getIndex("t17_ft"));
        assertEquals (1, table.getFullTextIndexes().size());
        FullTextIndex index = table.getFullTextIndexes().iterator().next();
        assertNotNull (table.getFullTextIndex("t17_ft"));
    }
    
    

}
