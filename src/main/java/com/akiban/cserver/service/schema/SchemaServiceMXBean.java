package com.akiban.cserver.service.schema;

import com.akiban.cserver.store.SchemaId;

import java.util.List;

public interface SchemaServiceMXBean {
    SchemaId getSchemaGeneration() throws Exception;
    List<String> getDDLs() throws Exception;
    void forceGenerationUpdate() throws Exception;
    String[] getAisAsString() throws Exception;
    void dropAllTables() throws Exception;
}
