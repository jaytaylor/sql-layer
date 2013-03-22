/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.optimizer.rule;

import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.rule.cost.CostEstimator;

import com.akiban.ais.model.AkibanInformationSchema;

import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;

/** The context associated with an AIS schema. */
public abstract class SchemaRulesContext extends RulesContext
{
    private Schema schema;

    private FunctionsRegistry functionsRegistry;
    private CostEstimator costEstimator;
    private T3RegistryService t3Registry;
    protected SchemaRulesContext() {
    }

    protected void initAIS(AkibanInformationSchema ais) {
        schema = SchemaCache.globalSchema(ais);
    }

    protected void initFunctionsRegistry(FunctionsRegistry functionsRegistry) {
        this.functionsRegistry = functionsRegistry;
    }

    protected void initT3Registry(T3RegistryService overloadResolver) {
        this.t3Registry = overloadResolver;
    }

    protected void initCostEstimator(CostEstimator costEstimator, boolean usePValues) {
        this.costEstimator = costEstimator;
    }

    @Override
    protected void initDone() {
        super.initDone();
        assert (schema != null) : "initSchema() not called";
        assert (functionsRegistry != null) : "initFunctionsRegistry() not called";
        assert (costEstimator != null) : "initCostEstimator() not called";
    }

    public Schema getSchema() {
        return schema;
    }

    public AkibanInformationSchema getAIS() {
        return schema.ais();
    }

    public PhysicalResultColumn getResultColumn(ResultField field) {
        return new PhysicalResultColumn(field.getName());
    }

    public T3RegistryService getT3Registry() {
        return t3Registry;
    }

    public FunctionsRegistry getFunctionsRegistry() {
        return functionsRegistry;
    }

    public CostEstimator getCostEstimator() {
        return costEstimator;
    }

    public abstract String getDefaultSchemaName();

}
