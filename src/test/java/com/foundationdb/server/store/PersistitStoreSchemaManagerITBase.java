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

package com.foundationdb.server.store;

import com.foundationdb.server.test.it.PersistitITBase;
import org.junit.Before;

import java.util.Map;

public class PersistitStoreSchemaManagerITBase extends PersistitITBase
{
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
