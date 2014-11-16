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

package com.foundationdb.server.service.text;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;

import java.util.Map;

public class FullTextIndexServiceITBase extends ITBase
{
    public static final String SCHEMA = "test";
    protected FullTextIndexServiceImpl fullTextImpl;
    protected Schema schema;
    protected StoreAdapter adapter;
    protected QueryContext queryContext;
    protected QueryBindings queryBindings;
    protected int c;
    protected int o;
    protected int i;
    protected int a;


    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    public Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }


    @Before
    public final void castService() {
        fullTextImpl = (FullTextIndexServiceImpl)serviceManager().getServiceByClass(FullTextIndexService.class);
    }

    protected void waitUpdate() {
        fullTextImpl.waitUpdateCycle();
    }
}
