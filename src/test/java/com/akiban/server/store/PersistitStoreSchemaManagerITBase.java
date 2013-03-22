
package com.akiban.server.store;

import com.akiban.server.test.it.ITBase;
import org.junit.Before;

import java.util.Map;

public class PersistitStoreSchemaManagerITBase extends ITBase {
    protected PersistitStoreSchemaManager pssm;

    private PersistitStoreSchemaManager castToPSSM() {
        SchemaManager schemaManager = serviceManager().getSchemaManager();
        if(schemaManager instanceof PersistitStoreSchemaManager) {
            return (PersistitStoreSchemaManager)schemaManager;
        } else {
            throw new IllegalStateException("Expected PersistitStoreSchemaManager!");
        }
    }

    @Before
    public void setUpPSSM() {
        pssm = castToPSSM();
    }

    protected void safeRestart() throws Exception {
        safeRestart(defaultPropertiesToPreserveOnRestart());
    }

    protected void safeRestart(Map<String, String> properties) throws Exception {
        pssm = null;
        safeRestartTestServices(properties);
        pssm = castToPSSM();
        updateAISGeneration();
    }
}
