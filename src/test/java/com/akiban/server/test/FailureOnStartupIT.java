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

package com.akiban.server.test;

import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.Store;
import com.google.inject.Inject;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public final class FailureOnStartupIT extends ApiTestBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider().bind(Store.class, BadStore.class);
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
        public BadStore(AisHolder aisHolder, TreeService treeService) {
            super(aisHolder, treeService, null);
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
