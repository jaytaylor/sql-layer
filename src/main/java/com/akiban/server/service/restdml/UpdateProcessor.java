/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.server.service.restdml;

import com.fasterxml.jackson.databind.JsonNode;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;

public class UpdateProcessor extends DMLProcessor {

    private final DeleteProcessor deleteProcessor;
    private final InsertProcessor insertProcessor;

    public UpdateProcessor(ConfigurationService configService,
            TreeService treeService, Store store,
            T3RegistryService t3RegistryService,
            DeleteProcessor deleteProcessor,
            InsertProcessor insertProcessor) {
        super(configService, treeService, store, t3RegistryService);
        this.deleteProcessor = deleteProcessor;
        this.insertProcessor = insertProcessor;
    }

    public String processUpdate (Session session, AkibanInformationSchema ais, TableName tableName, String values, JsonNode node) {
        deleteProcessor.processDelete(session, ais, tableName, values);
        return insertProcessor.processInsert(session, ais, tableName, node);
        
    }
}
