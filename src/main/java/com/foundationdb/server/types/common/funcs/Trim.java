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
package com.foundationdb.server.types.common.funcs;

import java.util.List;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class Trim extends TScalarBase {

    // Described by {L,R,}TRIM(<string_to_trim>, <char_to_trim>)
    public static TScalar[] create(TClass stringType) {
        TScalar rtrim = new Trim(stringType) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
                String st = inputs.get(0).getString();
                String trim = inputs.get(1).getString();
                
                if (!isValidInput(trim.length(), st.length())) {
                    output.putString(st, null);
                    return;
                }
                output.putString(rtrim(st, trim), null);
            }

            @Override
            public String displayName() {
                return "RTRIM";
            }
        };

        TScalar ltrim = new Trim(stringType) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
                String st = inputs.get(0).getString();
                String trim = inputs.get(1).getString();
                
                if (!isValidInput(trim.length(), st.length())) {
                    output.putString(st, null);
                    return;
                }
                output.putString(ltrim(st, trim), null);
            }

            @Override
            public String displayName() {
                return "LTRIM";
            }
        };

        TScalar trim = new Trim(stringType) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
                String st = inputs.get(0).getString();
                String trim = inputs.get(1).getString();
                
                if (!isValidInput(trim.length(), st.length())) {
                    output.putString(st, null);
                    return;
                }

                st = rtrim(ltrim(st, trim), trim);
                output.putString(st, null);
            }

            @Override
            public String displayName() {
                return "TRIM";
            }
        };
        
        return new TScalar[]{ltrim, rtrim, trim};
    }

    protected final TClass stringType;

    private Trim(TClass stringType) {
        this.stringType = stringType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(stringType, 0).covers(stringType, 1);
    }

    @Override
    public TOverloadResult resultType() {
        return customStringFromParameter(0);
    }

    // Helper methods
    protected static String ltrim(String st, String trim) {
        int n, count;
        n = count = 0;
        while (n < st.length()) {
            count = 0;
            for (int i = 0; i < trim.length() && n < st.length(); ++i, ++n) {
                if (st.charAt(n) != trim.charAt(i)) return st.substring(n-i);
                else count++;
            }
        }
        return count == trim.length() ? "" : st.substring(n-count);
    }

    protected static String rtrim(String st, String trim) {
        int n = st.length() - 1;
        int count = 0;
        while (n >= 0) {
            count = 0;
            for (int i = trim.length()-1; i >= 0 && n >= 0; --i, --n) {
                if (st.charAt(n) != trim.charAt(i))
                    return st.substring(0, n + trim.length() - i);
                else count++;
            }
        }
        return count == trim.length() ? "" : st.substring(0, count);
    }
    
    protected static boolean isValidInput(int trimLength, int strLength) {
        return trimLength != 0 && strLength != 0 && trimLength <= strLength;
    }
}
