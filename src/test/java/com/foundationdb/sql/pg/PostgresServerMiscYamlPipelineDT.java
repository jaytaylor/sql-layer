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

package com.foundationdb.sql.pg;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class PostgresServerMiscYamlPipelineDT extends PostgresServerMiscYamlIT {

    public final static String[] PIPELINE_PROPERTIES = {"fdbsql.pipeline.map.enabled=true",
                                                        "fdbsql.pipeline.indexScan.lookaheadQuantum=10",
                                                        "fdbsql.pipeline.groupLookup.lookaheadQuantum=10",
                                                        "fdbsql.pipeline.unionAll.openBoth=true",
                                                        "fdbsql.pipeline.selectBloomFilter.enabled=true"}; 

    public PostgresServerMiscYamlPipelineDT(String caseName, File file) {
        super(caseName, file);
    }
    
    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> superProperties = super.startupConfigProperties();
        Map<String, String> properties = new HashMap<String, String>();
        for (String property : PIPELINE_PROPERTIES) {
            String[] pieces = property.split("=");
            properties.put(pieces[0], pieces[1]);
        }
        properties.putAll(superProperties);
        return properties;
    }
}
