/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

package com.foundationdb.sql.pg;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.MemoryITBase;
import org.junit.Assume;

import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Run yamls with Memory storage. */
public class PostgresServerMemoryStoreYamlDT extends PostgresServerMiscYamlIT
{
    // These require features not present with memory storage
    // and are not easy to suppress via yaml
    private static final Set<String> SKIP_NAMES = new HashSet<>(Arrays.asList(
        "test-alter-column-keys",
        "test-alter-table-add-index",
        "test-create-table",
        "test-fdb-column-keys-format",
        "test-fdb-delayed-foreign-key",
        "test-fdb-delayed-uniqueness",
        "test-fdb-tuple-format",
        "test-show-param",
        "test-storage-format",
        "test-transaction-isolation-level",
        "test-pg-readonly4"
    ));

    public PostgresServerMemoryStoreYamlDT(String caseName, URL url) {
        super(caseName, url);
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return MemoryITBase.doBind(super.serviceBindingsProvider());
    }

    @Override
    protected void testYaml(URL url) throws Exception {
        for(String skip : SKIP_NAMES) {
            Assume.assumeFalse("Skipped", url.getPath().contains(skip));
        }
        super.testYaml(url);
    }
}
