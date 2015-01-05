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

package com.foundationdb.server.service.metrics;

import com.foundationdb.server.store.FDBHolder;
import com.foundationdb.server.test.it.FDBITBase;

import com.foundationdb.Transaction;
import com.foundationdb.async.Function;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

public class FDBMetricsServiceIT extends FDBITBase
{
    private FDBHolder fdbService;
    private FDBMetricsService metricsService;

    @Before
    public void wipeOutOld() {
        metricsService = fdbMetricsService();
        fdbService = fdbHolder();

        metricsService.completeBackgroundWork();
        fdbService.getTransactionContext().run(new Function<Transaction,Void>() {
                                         @Override
                                         public Void apply(Transaction tr) {
                                             tr.options().setAccessSystemKeys();
                                             metricsService.deleteBooleanMetric(tr, "TestBoolean");
                                             metricsService.deleteLongMetric(tr, "TestLong");
                                             return null;
                                         }
                                     });
        metricsService.reset();
    }

    @Test
    public void saveEnabled() {
        BooleanMetric testBoolean = metricsService.addBooleanMetric("TestBoolean");
        LongMetric testLong = metricsService.addLongMetric("TestLong");
        assertFalse(testBoolean.isEnabled());
        assertFalse(testLong.isEnabled());
        ((FDBMetric<Boolean>)testBoolean).setEnabled(true);
        ((FDBMetric<Long>)testLong).setEnabled(true);
        metricsService.completeBackgroundWork();
        metricsService.removeMetric(testBoolean);
        metricsService.removeMetric(testLong);
        testBoolean = metricsService.addBooleanMetric("TestBoolean");
        testLong = metricsService.addLongMetric("TestLong");
        assertTrue(testBoolean.isEnabled());
        assertTrue(testLong.isEnabled());
    }

    static class TestValues {
        List<FDBMetric.Value<Boolean>> booleanValues;
        List<FDBMetric.Value<Long>> longValues;
    }

    @Test
    public void saveMetrics() {
        BooleanMetric testBoolean = metricsService.addBooleanMetric("TestBoolean");
        LongMetric testLong = metricsService.addLongMetric("TestLong");
        ((FDBMetric<Boolean>)testBoolean).setEnabled(true);
        ((FDBMetric<Long>)testLong).setEnabled(true);
        testBoolean.set(false);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testBoolean.set(true);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testBoolean.toggle();
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testLong.set(100);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testLong.increment();
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        testLong.increment(-1);
        try { Thread.sleep(1); } catch (InterruptedException ex) {}
        metricsService.completeBackgroundWork();
        final FDBMetric<Boolean> m1 = (FDBMetric<Boolean>)testBoolean;
        final FDBMetric<Long> m2 = (FDBMetric<Long>)testLong;
        TestValues values = fdbService.getTransactionContext()
            .run(new Function<Transaction,TestValues> () {
                     @Override
                     public TestValues apply(Transaction tr) {
                         tr.options().setAccessSystemKeys();
                         TestValues values = new TestValues();
                         values.booleanValues = m1.readAllValues(tr).get();
                         values.longValues = m2.readAllValues(tr).get();
                         return values;
                     }
                 });
        checkValues(values.booleanValues, false, true, false);
        checkValues(values.longValues, 0L, 100L, 101L, 100L);
    }

    private <T> void checkValues(List<FDBMetric.Value<T>> values,
                                 Object... expected) {
        assertEquals("number of values", expected.length, values.size());
        long maxTime = System.currentTimeMillis() * 1000000;
        long minTime = maxTime - 5 * 1000 * 1000000;
        for (int i = 0; i < expected.length; i++) {
            FDBMetric.Value<T> value = values.get(i);
            assertEquals("value " + i + " of " + values, expected[i], value.value);
            assertTrue("time in range", (minTime < value.time) && (value.time < maxTime));
            minTime = value.time;
        }
    }
}
