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

import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.rowtype.Schema;

import java.util.List;

/** The context associated with an AIS schema. */
public class SchemaRulesContext extends RulesContext
{
    private Schema schema;

    public SchemaRulesContext(AkibanInformationSchema ais, List<BaseRule> rules) {
        super(rules);
        schema = new Schema(ais);
    }

    public Schema getSchema() {
        return schema;
    }

    public PhysicalResultColumn getResultColumn(String name, DataTypeDescriptor type,
                                                boolean nameDefaulted, Column column) {
        if ((column != null) && nameDefaulted)
            name = column.getName();
        return new PhysicalResultColumn(name);
    }

}
