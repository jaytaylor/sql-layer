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

import com.foundationdb.server.service.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** In-memory metric implementation when no durable alternative available. */
public class DummyMetricsService implements MetricsService, Service
{
    private final ConcurrentHashMap<String,BaseMetricImpl<?>> metrics = new ConcurrentHashMap<>();

    static abstract class BaseMetricImpl<T> implements BaseMetric<T> {
        private final String name;
        
        protected BaseMetricImpl(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
        
        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public String toString() {
            return name + " = " + getObject();
        }
    }

    static class BooleanMetricImpl extends BaseMetricImpl<Boolean> implements BooleanMetric {
        private final AtomicBoolean bvalue = new AtomicBoolean();

        public BooleanMetricImpl(String name) {
            super(name);
        }

        @Override
        public boolean get() {
            return bvalue.get();
        }

        @Override
        public void set(boolean value) {
            bvalue.set(value);
        }
        
        @Override
        public Boolean getObject() {
            return get();
        }

        @Override
        public void setObject(Boolean value) {
            set(value);
        }
        
        @Override
        public boolean toggle() {
            while (true) {
                boolean value = bvalue.get();
                boolean newValue = !value;
                if (bvalue.compareAndSet(value, newValue)) {
                    return newValue;
                }
            }
        }
    }

    static class LongMetricImpl extends BaseMetricImpl<Long> implements LongMetric {
        private final AtomicLong lvalue = new AtomicLong();

        public LongMetricImpl(String name) {
            super(name);
        }

        @Override
        public long get() {
            return lvalue.get();
        }

        @Override
        public void set(long value) {
            lvalue.set(value);
        }
        
        @Override
        public Long getObject() {
            return get();
        }

        @Override
        public void setObject(Long value) {
            set(value);
        }
        
        @Override
        public long increment() {
            return lvalue.incrementAndGet();
        }
        
        @Override
        public long increment(long amount) {
            return lvalue.addAndGet(amount);
        }
    }

    /* MetricCollection */

    @Override
    public BooleanMetric addBooleanMetric(String name) {
        BooleanMetricImpl metric = new BooleanMetricImpl(name);
        addMetric(metric);
        return metric;
    }

    @Override
    public LongMetric addLongMetric(String name) {
        LongMetricImpl metric = new LongMetricImpl(name);
        return metric;
    }

    @Override
    public <T> void removeMetric(BaseMetric<T> metric) {
        metrics.remove(metric.getName(), metric);
    }
    
    /* Service */
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }

    /* Internal */

    protected void addMetric(BaseMetricImpl<?> metric) {
        if (metrics.putIfAbsent(metric.getName(), metric) != null) {
            throw new IllegalArgumentException("There is already a metric named " + metric.getName());
        }
    }
}
