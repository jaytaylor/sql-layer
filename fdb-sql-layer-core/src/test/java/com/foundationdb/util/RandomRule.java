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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Contains a random value that is optionally seeded by the environment variable:
 * fdbsql.test.seed
 *
 * Add a new Rule to your test class to get a random generator:
 * <code>
 *   @Rule
 *   public final RandomRule randomRule = new RandomRule();
 * </code>
 *
 * If you want to access this in the @Parameters, use the @ClassRule attribute and make it static; assign the @Rule field
 * to the same value
 * <code>
 *   @ClassRule
 *   public static final RandomRule classRandom = new RandomRule();
 *   @Rule
 *   public final RandomRule randomRule = classRandom;
 * </code>
 *
 *   Also, you should call reset() at the top of the parameters. I don't know why but intellij is really weird, possibly
 *   broken if you try to run a single parameter it will run the parameters method twice, and then look for the test
 *   in the second run, which will be different if you don't reset the seed.
 *
 *
 *
 * It will reset the seed before every test, and before setting up the class.
 */
public class RandomRule implements TestRule {

    private static final Logger LOG = LoggerFactory.getLogger(RandomRule.class);

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

    @Override
    public Statement apply(final Statement statement, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                // We want to reseed, because we don't want the values going into the test to be tainted by how many
                // tests ran before
                reset();
                boolean success = false;
                try {
                    // Note: this will include execution of before/after methods
                    statement.evaluate();
                    success = true;
                } finally {
                    if (!success) {
                        // This only prints if the @Rule attribute is used, not if @ClassRule is used by itself
                        LOG.error("Test ({}) failed with seed {}", description.getDisplayName(), seed); 
                    }
                }
            }
        };
    }

    public Random reset() {
        random.setSeed(seed);
        return random;
    }

    public Random getRandom() {
        return random;
    }
}
