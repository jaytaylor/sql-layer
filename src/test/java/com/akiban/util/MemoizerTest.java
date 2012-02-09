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

package com.akiban.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class MemoizerTest {
    @Test
    public void standardMemoizer() {
        IntToLong intToLong = new IntToLong() {
            @Override
            protected Long doCompute(Integer input) {
                return (long) input * 2;
            }
        };
        assertEquals("doubled 4", Long.valueOf(8), intToLong.get(4));
        assertEquals("doubler messages", Collections.singletonList(4), intToLong.messages);
    }
    
    @Test
    public void nullInput() {
        IntToLong intToLong = new IntToLong() {
            @Override
            protected Long doCompute(Integer input) {
                return 8L;
            }
        };
        assertEquals("doubled 4", Long.valueOf(8), intToLong.get(null));
        assertEquals("doubler messages", Collections.singletonList(null), intToLong.messages);
    }

    @Test
    public void nullOutput() {
        IntToLong intToLong = new IntToLong() {
            @Override
            protected Long doCompute(Integer input) {
                return null;
            }
        };
        assertEquals("doubled 4", null, intToLong.get(3));
        assertEquals("doubler messages", Collections.singletonList(3), intToLong.messages);
    }
    
    @Test
    public void resultSet() {
        IntToLong intToLong = new IntToLong() {
            @Override
            protected Long doCompute(Integer input) {
                throw new UnsupportedComputationException();
            }
        };

        // the value for 4 is manually set, so it should be available without having to compute
        intToLong.set(4, 8L);
        assertEquals("doubled 4", Long.valueOf(8), intToLong.get(4));
        assertEquals("doubler messages", Collections.emptyList(), intToLong.messages);

        // the value for 3 hasn't been set, so it should be computed -- resulting in an exception
        try {
            intToLong.get(3);
            fail("expected " + UnsupportedComputationException.class.getSimpleName());
        } catch (UnsupportedComputationException e) {
            // expected
        }
        assertEquals("doubler messages", Collections.singletonList(3), intToLong.messages);
    }

    private abstract static class IntToLong extends Memoizer<Integer,Long> {
        @Override
        protected final Long compute(Integer input) {
            messages.add(input);
            return doCompute(input);
        }
        
        protected abstract Long doCompute(Integer input);

        private List<Integer> messages = new ArrayList<Integer>(1);
    }
    
    private static class UnsupportedComputationException extends UnsupportedOperationException {}
}
