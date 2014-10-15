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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.common.types.TypesTranslator;

/** The context associated with an AIS schema. */
public abstract class SchemaRulesContext extends RulesContext
{
    private Schema schema;

    private CostEstimator costEstimator;
    private PipelineConfiguration pipelineConfiguration;
    private TypesRegistryService typesRegistry;
    private TypesTranslator typesTranslator;

    protected SchemaRulesContext() {
    }

    protected void initAIS(AkibanInformationSchema ais) {
        schema = SchemaCache.globalSchema(ais);
    }
     
    protected void initCostEstimator(CostEstimator costEstimator) {
        this.costEstimator = costEstimator;
    }

    protected void initPipelineConfiguration(PipelineConfiguration pipelineConfiguration) {
        this.pipelineConfiguration = pipelineConfiguration;
    }

    protected void initTypesRegistry(TypesRegistryService typesRegistry) {
        this.typesRegistry = typesRegistry;
    }

    protected void initTypesTranslator(TypesTranslator typesTranslator) {
        this.typesTranslator = typesTranslator;
    }

    @Override
    protected void initDone() {
        super.initDone();
        assert (schema != null) : "initSchema() not called";
        assert (costEstimator != null) : "initCostEstimator() not called";
        assert (pipelineConfiguration != null) : "initPipelineConfiguration() not called";
        assert (typesRegistry != null) : "initTypesRegistry() not called";
        assert (typesTranslator != null) : "initTypesTranslator() not called";
    }

    public Schema getSchema() {
        return schema;
    }

    public AkibanInformationSchema getAIS() {
        return schema.ais();
    }

    public abstract String getDefaultSchemaName();

    public CostEstimator getCostEstimator() {
        return costEstimator;
    }

    public PipelineConfiguration getPipelineConfiguration() {
        return pipelineConfiguration;
    }

    public TypesRegistryService getTypesRegistry() {
        return typesRegistry;
    }

    public TypesTranslator getTypesTranslator() {
        return typesTranslator;
    }

    public PhysicalResultColumn getResultColumn(ResultField field) {
        return new PhysicalResultColumn(field.getName());
    }

}
