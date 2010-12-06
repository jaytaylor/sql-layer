package com.akiban.cserver.service.schema;

import com.akiban.ais.io.CSVTarget;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.manage.SchemaManager;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.store.SchemaId;

import java.util.List;

public final class SchemaServiceImpl implements SchemaService, JmxManageable, Service<SchemaService> {
    private final SchemaManager manager;

    public SchemaServiceImpl(SchemaManager manager) {
        this.manager = manager;
    }

    @Override
    public SchemaId getSchemaGeneration() throws Exception {
        return manager.getSchemaID();
    }

    @Override
    public List<String> getDDLs() throws Exception {
        return manager.getDDLs();
    }

    @Override
    public void forceGenerationUpdate() throws Exception {
        manager.forceSchemaGenerationUpdate();
    }

    public String[] getAisAsString() throws Exception {
        AkibaInformationSchema ais = manager.getAisCopy();
        return CSVTarget.toString(ais).split("\\n");
    }

    @Override
    public void dropAllTables() throws Exception {
        manager.dropAllTables();
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Schema", this, SchemaServiceMXBean.class);
    }

    @Override
    public void start() throws Exception {
        // no-op
    }

    @Override
    public void stop() throws Exception {
        // no-op
    }

    @Override
    public SchemaService cast() {
        return this;
    }

    @Override
    public Class<SchemaService> castClass() {
        return SchemaService.class;
    }
}
