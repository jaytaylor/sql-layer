/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Random;

/**
 * Contains a random value that is optionally seeded by the environment variable:
 * fdbsql.test.seed
 *
 * If you want to access this in the @Parameters, use the @ClassRule attribute and make it static
 * If you want to access this in the tests,before or after, use the @Rule attribute
 * If you want both assign the field with the @Rule attribute to the @ClassRule field.
 *
 * It will reset the seed before every test, and before setting up the class.
 */
public class RandomRule implements TestRule {

    private long seed;
    private Random random;

    public RandomRule() {
        String seedStr = System.getProperty("fdbsql.test.seed");
        if (seedStr == null) {
            seed = System.currentTimeMillis();
        } else {
            seed = Long.parseLong(seedStr);
        }
        random = new Random(seed);
    }

    public long nextLong() {
        return getRandom().nextLong();
    }

    @Override
    public Statement apply(final Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                // We want to reseed, because we don't want the values going into the test to be tainted by how many
                // tests ran before
                random.setSeed(seed);
                boolean success = false;
                try {
                    // Note: this will include execution of before/after methods
                    statement.evaluate();
                    success = true;
                } finally {
                    if (!success) {
                        System.err.printf("Test failed with seed %d\n", seed);
                    }
                }
            }
        };
    }

    public Random getRandom() {
        return random;
    }
}
