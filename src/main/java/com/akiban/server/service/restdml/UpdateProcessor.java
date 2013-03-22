package com.akiban.server.service.restdml;

import org.codehaus.jackson.JsonNode;

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
        setAIS (ais);
        deleteProcessor.processDelete(session, ais, tableName, values);
        return insertProcessor.processInsert(session, ais, tableName, node);
        
    }
}
