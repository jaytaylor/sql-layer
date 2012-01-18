/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.rule;

import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;

import com.akiban.ais.model.AkibanInformationSchema;

import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;

import java.util.List;
import java.util.Properties;

/** The context associated with an AIS schema. */
public class SchemaRulesContext extends RulesContext
{
    private Schema schema;
    private FunctionsRegistry functionsRegistry;
    private IndexEstimator indexEstimator;

    public SchemaRulesContext(AkibanInformationSchema ais,
                              FunctionsRegistry functionsRegistry,
                              IndexEstimator indexEstimator,
                              List<BaseRule> rules,
                              Properties properties) {
        super(rules, properties);
        schema = SchemaCache.globalSchema(ais);
        this.functionsRegistry = functionsRegistry;
        this.indexEstimator = indexEstimator;
    }

    public Schema getSchema() {
        return schema;
    }

    public PhysicalResultColumn getResultColumn(ResultField field) {
        return new PhysicalResultColumn(field.getName());
    }

    public FunctionsRegistry getFunctionsRegistry() {
        return functionsRegistry;
    }
      
    public IndexEstimator getIndexEstimator() {
        return indexEstimator;
    }

}
