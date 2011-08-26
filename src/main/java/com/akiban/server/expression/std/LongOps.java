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

package com.akiban.server.expression.std;

import com.akiban.server.types.AkType;

public class LongOps {

    public static LongOp LONG_MULTIPLY = new LongOpForLong("*") {
        @Override
        public long evaluate(long one, long two) {
            return one * two;
        }
    };

    public static LongOp LONG_SUBTRACT = new LongOpForLong("-") {
        @Override
        public long evaluate(long one, long two) {
            return one - two;
        }
    };

    public static LongOp LONG_ADD = new LongOpForLong("+") {
        @Override
        public long evaluate(long one, long two) {
            return one + two;
        }
    };

    private LongOps() {}

    private static abstract class LongOpForLong implements LongOp {
        @Override
        public AkType opType() {
            return AkType.LONG;
        }

        @Override
        public String toString() {
            return name;
        }

        protected LongOpForLong(String name) {
            this.name = name;
        }

        private final String name;
    }
}
