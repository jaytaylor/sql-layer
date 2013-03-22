
package com.akiban.server.test;

import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.google.inject.Inject;
import org.junit.Test;
import java.util.Map;

import static org.junit.Assert.assertFalse;

public final class FailureOnStartupIT extends ApiTestBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider().bind(Store.class, BadStore.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Test
    public void one() {
        // failure would have happened in @Before
        assertFalse(stage == Stage.FIRST_START);
    }

    @Test
    public void two() {
        // failure would have happened in @Before, but hopefully not at all!
        // we can't assert that stage is SUBSEQUENT, because JUnit doesn't guarantee ordering
        assertFalse(stage == Stage.FIRST_START);
    }

    public FailureOnStartupIT() {
        super("IT");
    }

    @Override
    void handleStartupFailure(Exception e) throws Exception {
        // eat only the first failure
        if (stage == Stage.FIRST_FAILURE) {
            stage = Stage.SUBSEQUENT;
            return;
        }
        throw e;
    }

    private static Stage stage = Stage.FIRST_START;

    private enum Stage {
        FIRST_START,
        FIRST_FAILURE,
        SUBSEQUENT
    }

    public static class BadStore extends OperatorStore {

        @Inject
        public BadStore(TreeService treeService, SchemaManager schemaManager, LockService lockService,
                        TransactionService transactionService) {
            super(treeService, null, schemaManager, lockService, transactionService);
        }

        @Override
        public void start() {
            if (stage == Stage.FIRST_START) {
                stage = Stage.FIRST_FAILURE;
                throw new RuntimeException();
            }
            super.start();
        }
    }


}
