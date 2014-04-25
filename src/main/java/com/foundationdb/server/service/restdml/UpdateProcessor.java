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
package com.foundationdb.server.service.restdml;

import com.fasterxml.jackson.databind.JsonNode;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.service.TypesRegistryService;

public class UpdateProcessor extends DMLProcessor {

    private final DeleteProcessor deleteProcessor;
    private final InsertProcessor insertProcessor;

    public UpdateProcessor(Store store, SchemaManager schemaManager,
            TypesRegistryService typesRegistryService,
            DeleteProcessor deleteProcessor,
            InsertProcessor insertProcessor) {
        super(store, schemaManager, typesRegistryService);
        this.deleteProcessor = deleteProcessor;
        this.insertProcessor = insertProcessor;
    }

    public String processUpdate (Session session, AkibanInformationSchema ais, TableName tableName, String values, JsonNode node) {
        deleteProcessor.processDelete(session, ais, tableName, values);
        return insertProcessor.processInsert(session, ais, tableName, node);
        
    }
}
