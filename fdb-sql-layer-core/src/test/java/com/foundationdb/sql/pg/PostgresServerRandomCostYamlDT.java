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

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;

import com.foundationdb.sql.optimizer.rule.cost.CostModelFactory;
import com.foundationdb.sql.optimizer.rule.cost.RandomCostModelService;
import org.junit.Test;

import java.io.File;

/**
 * Run tests specified as YAML files that end with the .yaml extension.  By
 * default, searches for files recursively in the yaml resource directory,
 * running tests for files that start with 'test-'.  Tests will be run with
 * Random Cost Model in order to cause alternative operator plans to test correctness
 */
public class PostgresServerRandomCostYamlDT extends PostgresServerMiscYamlIT
{
    public PostgresServerRandomCostYamlDT(String caseName, File file) {
        super(caseName, file);
    }

    @Override
    protected boolean isRandomCost(){
        return true;
    }
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(CostModelFactory.class, RandomCostModelService.class);
        }

    @Test
    public void testYaml() throws Exception {
        boolean  success = false;
        try {
            ((RandomCostModelService) serviceManager().getServiceByClass(CostModelFactory.class)).reSeed();
            super.testYaml();
            success = true;
        } finally {
            if(success == false){
                System.err.printf("\nFailed when ran with random seed: %d \n",
                        ((RandomCostModelService) serviceManager().getServiceByClass(CostModelFactory.class)).getSeed()
                );
            }
        }
    }

}
