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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.service.functions.FunctionsRegistryImpl;

import java.util.List;
import java.util.Properties;
import java.io.File;
import java.io.IOException;

public class RulesTestContext extends SchemaRulesContext
{
    public RulesTestContext(AkibanInformationSchema ais, String defaultSchema, 
                            File statsFile, List<? extends BaseRule> rules, Properties properties)
            throws IOException {
        super(ais, 
              new FunctionsRegistryImpl(), 
              new TestCostEstimator(ais, defaultSchema, statsFile),
              rules, properties);
        RulesTestHelper.ensureRowDefs(ais);
    }
}
