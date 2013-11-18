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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.PersistitITBase;

import java.util.Collections;
import java.util.Map;

/** Artificially low periodic committing. **/
public class PersistitPeriodicCommitIT extends AlterTableBasicIT
{
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return PersistitITBase.doBind(super.serviceBindingsProvider());
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return Collections.singletonMap("fdbsql.persistit.periodically_commit.after_millis", "1");
    }
}
