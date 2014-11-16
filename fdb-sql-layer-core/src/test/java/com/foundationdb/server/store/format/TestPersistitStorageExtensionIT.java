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

package com.foundationdb.server.store.format;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.it.ITBase;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestPersistitStorageExtensionIT extends ITBase
{
    protected boolean wasPersistit;

    @Before
    public void register() {
        StorageFormatRegistry storageFormatRegistry =
            serviceManager().getSchemaManager().getStorageFormatRegistry();
        wasPersistit = (storageFormatRegistry instanceof PersistitStorageFormatRegistry);
        if (!wasPersistit) return;
        TestPersistitStorageFormat.register(storageFormatRegistry);
    }

    @Test
    public void createWithTest() {
        if (!wasPersistit) return;

        createFromDDL("test",
                      "CREATE TABLE t1(id INT PRIMARY KEY NOT NULL) STORAGE_FORMAT test(name = 'Fred', option = true)");
        Group group = ais().getGroup(new TableName("test", "t1"));
        StorageDescription storage = group.getStorageDescription();
        assertTrue(storage instanceof TestPersistitStorageDescription);
        TestPersistitStorageDescription testStorage = (TestPersistitStorageDescription)storage;
        assertEquals("Fred", testStorage.getName());
        assertEquals("true", testStorage.getOption());
    }
    
}
