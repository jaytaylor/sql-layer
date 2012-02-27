/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.server.test.it.rowtests;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;

public class OldTypeITBase extends ITBase {
    protected static final String SCHEMA = "test";
    protected static final String TABLE = "t";
    private boolean createIndexes = false;

    @Before
    public final void defaultCreateIndexes() {
        createIndexes = false;
    }

    protected final void setCreateIndexes(boolean createIndexes) {
        this.createIndexes = createIndexes;
    }

    protected static final class TypeAndParams {
        public final String typeName;
        public final Long param1;
        public final Long param2;

        public TypeAndParams(String typeName, Long param1, Long param2) {
            this.typeName = typeName;
            this.param1 = param1;
            this.param2 = param2;
        }
    }

    protected final int createTableFromTypes(TypeAndParams... typeAndParams) {
        AISBuilder builder = new AISBuilder();
        builder.userTable(SCHEMA, TABLE);
        int colPos = 0;
        builder.column(SCHEMA, TABLE, "id", colPos++, "INT", null, null, false, false, null, null);

        for(TypeAndParams type : typeAndParams) {
            String name = "c" + (colPos + 1);
            builder.column(SCHEMA, TABLE, name, colPos++, type.typeName, type.param1, type.param2, true, false, null, null);

            if(createIndexes) {
                builder.index(SCHEMA, TABLE, name, false, Index.KEY_CONSTRAINT);
                builder.indexColumn(SCHEMA, TABLE, name, name, 0, true, null);
            }
        }

        UserTable tempTable = builder.akibanInformationSchema().getUserTable(SCHEMA, TABLE);
        ddl().createTable(session(), tempTable);
        updateAISGeneration();
        return tableId(SCHEMA, TABLE);
    }

    protected final int createTableFromTypes(String... typeNames) {
        TypeAndParams[] typeAndParams = new TypeAndParams[typeNames.length];
        for(int i = 0; i < typeNames.length; ++i) {
            typeAndParams[i] = new TypeAndParams(typeNames[i], null, null);
        }
        return createTableFromTypes(typeAndParams);
    }
}
