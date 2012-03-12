/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.daily.slap;

import static org.junit.Assert.assertTrue;

import com.akiban.server.service.config.Property;
import com.akiban.server.store.PersistitStoreSchemaManager;
import org.junit.Test;

import com.akiban.ais.model.TableName;
import com.akiban.server.test.daily.DailyBase;

import java.util.Collection;
import java.util.Collections;

/**
 * This test simply creates and then drops 2000 tables. Prior to the
 * fix for 772047 this caused an OOME.  Completion of this test constitutes
 * success.
 * @author peter
 *
 */
public class LotsOfTablesDT extends DailyBase {
    private final static int TABLE_COUNT = 2000;

    @Override
    protected Collection<Property> startupConfigProperties() {
        // Set no limit on maximum serialized AIS size
        return Collections.singleton(new Property(PersistitStoreSchemaManager.MAX_AIS_SIZE_PROPERTY, "0"));
    }

    @Test
    public void createLotsOfTablesTest() throws Exception {
        int was = -1;
        for (int count = 0; count < TABLE_COUNT; count++) {
            String tableName = String.format("test%04d", count);
            int tableId = createTable("test", tableName,
                    "I INT NOT NULL, V VARCHAR(255), PRIMARY KEY(I)");
            assertTrue(was != tableId);
            was = tableId;
        }

        for (int count = 0; count < TABLE_COUNT; count++) {
            String tableName = String.format("test%04d", count);
            ddl().dropTable(session(), new TableName("test", tableName));
        }
    }
}
