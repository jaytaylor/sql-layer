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

package com.foundationdb.server.test.daily.slap;

import com.foundationdb.server.test.daily.DailyBase;
import org.junit.Test;

public final class LotsOfServicesDT extends DailyBase {
    @Test
    public void loop() throws Throwable {

        stopTestServices(); // shut down ApiTestBase's @Before services

        final int LOOP_COUNT = 1000;
        int i=0;
        try {
            for (; i < LOOP_COUNT; ++i) {
                startTestServices();
                Thread.sleep(10);
                stopTestServices();
            }
        } catch (Throwable e) {
            throw new RuntimeException("At i="+i, e);
        }

        startTestServices(); // so that ApiTestBase's @After has something to shut down
    }
}
