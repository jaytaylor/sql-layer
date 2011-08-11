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

package com.akiban.server.types.typestests;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionTarget;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Random;

import static org.junit.Assert.fail;

abstract class MismatchedConversion<T> {

    // MismatchedConversion interface

    public final void expectMismatch(T instance) {
        Method method = method(conversionType, conversionGets);
        final Object[] state;
        Class<?>[] argTypes = method.getParameterTypes();
        if (argTypes.length == 0) {
            state = new Object[0];
        }
        else if (argTypes.length == 1) {
            state = state(argTypes[0]);
        } else {
            throw new RuntimeException("args must be of 0 or 1 type: " + Arrays.toString(argTypes));
        }
        boolean failed = false;
        try {
            method.invoke(instance, state);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (Throwable t) {
            failed = true;
        }

        if (!failed) {
            fail(complain() + ", state=" + Arrays.toString(state));
        }
    }

    // for use by subclasses

    protected abstract Object[] state(Class<?> ofType);
    protected abstract String complain();

    protected MismatchedConversion(AkType conversionExpects, ConversionType conversionType) {
        this.conversionExpects = conversionExpects;
        this.conversionGets = not(conversionExpects);
        this.conversionType = conversionType;
    }

    // object state

    final AkType conversionExpects;
    final AkType conversionGets;
    final ConversionType conversionType;

    // static methods for use in this class

    private static AkType not(AkType expected) {
        Random rand = new Random(System.currentTimeMillis());
        AkType actual = expected;
        while (actual == expected || illegalTypes.contains(actual)) {
            int randOrd = rand.nextInt(AkType.values().length);
            actual = AkType.values()[randOrd];
        }
        return actual;
    }

    private static Collection<AkType> illegalTypes() {
        return EnumSet.of(AkType.NULL, AkType.UNSUPPORTED);
    }

    private static Collection<AkType> illegalTypes = illegalTypes();

    private static Method method(ConversionType conversionType, AkType akType) {
        final String suffix;
        switch (akType) {
        case DATETIME:
            suffix = "DateTime";
            break;
        case U_BIGINT:
            suffix = "UBigInt";
            break;
        case VARBINARY:
            suffix = "VarBinary";
            break;
        case VARCHAR:
            suffix = "String";
            break;
        case NULL:
        case UNSUPPORTED:
            throw new UnsupportedOperationException(akType + " is invalid here");
        default:
            StringBuilder builder = new StringBuilder(akType.name().toLowerCase());
            builder.setCharAt(0, Character.toUpperCase(builder.charAt(0)));
            // replace _x with X
            for(int charIndex = builder.indexOf("_"); charIndex >= 0; charIndex = builder.indexOf("_")) {
                builder.deleteCharAt(charIndex);
                builder.setCharAt(charIndex, Character.toUpperCase(builder.charAt(charIndex)));
            }
            suffix = builder.toString();
        }

        String methodName = conversionType.prefix + suffix;

        for (Method method : conversionType.ofClass.getDeclaredMethods()) {
            if (method.getName().endsWith(methodName)) {
                return method;
            }
        }
        throw new UnsupportedOperationException("couldn't find method " + conversionType.ofClass + " :: " + methodName);
    }

    // nested classes

    private enum ConversionType {
        SOURCE("get", ConversionSource.class),
        TARGET("put", ConversionTarget.class)
        ;

        private ConversionType(String prefix, Class<?> ofClass) {
            this.prefix = prefix;
            this.ofClass = ofClass;
        }

        final String prefix;
        final Class<?> ofClass;
    }

    static class ForGet extends MismatchedConversion<ConversionSource> {
        @Override
        protected Object[] state(Class<?> ofType) {
            return new Object[0];
        }

        @Override
        protected String complain() {
            return "expected failure while getting "
                    + conversionGets
                    + " after telling source we'd get "
                    + conversionExpects;
        }

        ForGet(AkType sourceExpects) {
            super(sourceExpects, ConversionType.SOURCE);
        }
    }

    static class ForPut extends MismatchedConversion<ConversionTarget> {
        @Override
        protected Object[] state(Class<?> ofType) {
            if (ofType == long.class)
                return new Object[]{ 1L };
            if (ofType == double.class) {
                return new Object[] { 1D };
            }
            if (ofType == float.class) {
                return new Object[] { 1F };
            }
            if (ofType == String.class) {
                return new Object[] { "foo" };
            }
            if (ofType == BigDecimal.class) {
                return new Object[] { BigDecimal.ZERO };
            }
            if (ofType == BigInteger.class) {
                return new Object[] { BigInteger.ZERO };
            }
            if (ofType == ByteSource.class) {
                return new Object[] { new WrappingByteSource().wrap(new byte[1]) };
            }
            throw new UnsupportedOperationException(ofType.getName());
        }

        @Override
        protected String complain() {
            return "expected failure while putting "
                    + conversionGets
                    + " after telling target we'd put "
                    + conversionExpects;
        }

        ForPut(AkType conversionExpects) {
            super(conversionExpects, ConversionType.TARGET);
        }
    }
}
