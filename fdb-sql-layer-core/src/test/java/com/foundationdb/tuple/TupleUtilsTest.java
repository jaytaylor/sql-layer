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

package com.foundationdb.tuple;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

import com.foundationdb.tuple.TupleUtil.DecodeResult;

import static org.junit.Assert.*;

public class TupleUtilsTest {

    @Test
    public void doubleEncodingTest() {
        Double initDouble = 4.5;
        byte[] bytes = TupleFloatingUtil.encode(initDouble);
        DecodeResult result = TupleFloatingUtil.decodeDouble(bytes, 1);
        assertEquals(initDouble, (Double) result.o);
        
        initDouble = -4.5;
        bytes = TupleFloatingUtil.encode(initDouble);
        result = TupleFloatingUtil.decodeDouble(bytes, 1);
        assertEquals(initDouble, (Double) result.o);
        
        initDouble = 0.0;
        bytes = TupleFloatingUtil.encode(initDouble);
        result = TupleFloatingUtil.decodeDouble(bytes, 1);
        assertEquals(initDouble, (Double) result.o);
    }
    
    @Test
    public void floatEncodingTest() {
        Float initFloat = (float) 4.5;
        byte[] bytes = TupleFloatingUtil.encode(initFloat);
        DecodeResult result = TupleFloatingUtil.decodeFloat(bytes, 1);
        assertEquals(initFloat, (Float) result.o);
        
        initFloat = (float) -4.5;
        bytes = TupleFloatingUtil.encode(initFloat);
        result = TupleFloatingUtil.decodeFloat(bytes, 1);
        assertEquals(initFloat, (Float) result.o);
        
        initFloat = (float) 0.0;
        bytes = TupleFloatingUtil.encode(initFloat);
        result = TupleFloatingUtil.decodeFloat(bytes, 1);
        assertEquals(initFloat, (Float) result.o);
        
        bytes = TupleFloatingUtil.floatingPointToByteArray((float) -42);
        bytes = TupleFloatingUtil.floatingPointCoding(bytes, true);
        assertEquals(ByteArrayUtil.printable(bytes), "=\\xd7\\xff\\xff");
    }
    
    @Test
    public void bigIntEncodingTest() {
        BigInteger bigInteger = new BigInteger("12345678912345");
        byte[] bytes = TupleFloatingUtil.encode(bigInteger);
        DecodeResult result = TupleFloatingUtil.decodeBigInt(bytes, 1);
        assertEquals(bigInteger, (BigInteger) result.o);
        
        bigInteger = new BigInteger("-12345678912345");
        bytes = TupleFloatingUtil.encode(bigInteger);
        result = TupleFloatingUtil.decodeBigInt(bytes, 1);
        assertEquals(bigInteger, (BigInteger) result.o);
    }
    
    @Test
    public void bigDecEncodingTest() {
        BigDecimal bigDecimal = new BigDecimal("123456789.123456789");
        byte[] bytes = TupleFloatingUtil.encode(bigDecimal);
        DecodeResult result = TupleFloatingUtil.decodeBigDecimal(bytes, 1);
        assertEquals(bigDecimal, (BigDecimal) result.o);
        
        bigDecimal = new BigDecimal("-123456789.123456789");
        bytes = TupleFloatingUtil.encode(bigDecimal);
        result = TupleFloatingUtil.decodeBigDecimal(bytes, 1);
        assertEquals(bigDecimal, (BigDecimal) result.o);
    }

    @Test
    public void booleanEncodingTest() {
        Boolean bool = new Boolean(true);
        byte[] bytes = TupleFloatingUtil.encode(bool);
        DecodeResult result = TupleFloatingUtil.decode(bytes, 0, 1);
        assertEquals(bool, (Boolean) result.o);

        bool = new Boolean(false);
        bytes = TupleFloatingUtil.encode(bool);
        result = TupleFloatingUtil.decode(bytes, 0, 1);
        assertEquals(bool, (Boolean) result.o);
    }
}
