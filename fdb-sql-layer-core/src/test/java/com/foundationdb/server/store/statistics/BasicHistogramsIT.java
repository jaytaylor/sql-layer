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

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.Strings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class BasicHistogramsIT extends ITBase
{
    private static final String SCHEMA = "test";
    private static final File RESOURCE_DIR = new File("src/test/resources/" +
                                                      BasicHistogramsIT.class.getPackage().getName().replace('.', '/') +
                                                      "/histograms");

    private final String bucketCount;
    private final File expectedFile;

    @TestParameters
    public static Collection<Parameterization> types() throws Exception {
        String[] sizes = { "32", "256" };
        Parameterization[] params = new Parameterization[sizes.length];
        for(int i = 0; i < sizes.length; ++i) {
            params[i] = Parameterization.create("buckets_"+sizes[i],
                                                sizes[i],
                                                "stats_"+sizes[i]+".yaml");
        }
        return Arrays.asList(params);
    }

    public BasicHistogramsIT(String bucketCount, String expectedFile) {
        this.bucketCount = bucketCount;
        this.expectedFile = new File(RESOURCE_DIR, expectedFile);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put(TestConfigService.BUCKET_COUNT_KEY, bucketCount);
        return config;
    }

    @Before
    public void load() throws Exception {
        loadDatabase(SCHEMA, RESOURCE_DIR);
    }

    @Test
    public void test() throws IOException {
        final Set<Index> indexes = new HashSet<>();
        for(Table t : ais().getSchema(SCHEMA).getTables().values()) {
            indexes.addAll(t.getIndexes());
            indexes.addAll(t.getGroup().getIndexes());

        }
        txnService().run(session(), new Runnable() {
            @Override
            public void run() {
                indexStatsService().updateIndexStatistics(session(), indexes);
            }
        });
                
        // The read of index statistics is done through a snapshot view,
        // so commit the changes before trying to read them. 
        String actual = txnService().run(session(), new Callable<String>() {
            @Override
            public String call() throws IOException {
                StringWriter writer = new StringWriter();
                indexStatsService().dumpIndexStatistics(session(), SCHEMA, writer);
                return writer.toString();
            }
        });
        String expected = Strings.dumpFileToString(expectedFile);
        assertEquals(strip(expected), strip(actual));
    }

    private IndexStatisticsService indexStatsService() {
        return serviceManager().getServiceByClass(IndexStatisticsService.class);
    }

    private String strip(String s) {
        return s.replace("\r", "").trim().replaceAll("Timestamp: .*Z", "Timestamp: null");
    }
}
