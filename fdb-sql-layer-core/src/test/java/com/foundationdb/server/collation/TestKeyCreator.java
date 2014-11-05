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

package com.foundationdb.server.collation;

import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.ValuesHKey;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.persistit.Key;

public class TestKeyCreator implements KeyCreator {

    public TestKeyCreator (Schema schema) {
        this.schema = schema;
        TypesRegistryServiceImpl registryImpl = new TypesRegistryServiceImpl();
        registryImpl.start();
        registry = registryImpl;
    }

    public Key createKey() {
        return new Key(null, 2047);
    }

    @Override
    public HKey newHKey(com.foundationdb.ais.model.HKey hKeyMetadata) {
        return new ValuesHKey(schema.newHKeyRowType(hKeyMetadata), registry);
    }
    private final Schema schema;
    private TypesRegistryService registry;

}
