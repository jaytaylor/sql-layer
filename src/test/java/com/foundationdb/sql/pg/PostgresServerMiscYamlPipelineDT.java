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
        Map<String, String> properties = new HashMap<>();
        for (String property : PIPELINE_PROPERTIES) {
            String[] pieces = property.split("=");
            properties.put(pieces[0], pieces[1]);
        }
        return properties;
    }
}
