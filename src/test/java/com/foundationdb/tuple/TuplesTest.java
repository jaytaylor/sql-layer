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
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

public class TuplesTest {

    @Test
    public void tuplesTest() {
        
        Tuple2 t = new Tuple2();
        t = t.add(Long.MAX_VALUE);
        t = t.add(1);
        t = t.add(0);
        t = t.add(-1);
        t = t.add(Long.MIN_VALUE);
        t = t.add("foo");
        t = t.add(4.5);
        t = t.add((Float) (float) 4.5);
        t = t.add((Float) (float) -4.5);
        t = t.add(new Boolean(true));
        t = t.add(new BigInteger("123456789123456789"));
        t = t.add(new BigDecimal("123456789.123456789"));
        t = t.add(new BigDecimal("-12345678912345.1234567891234"));
        t = t.add(new Boolean(false));
        byte[] bytes = t.pack();
        List<Object> items = Tuple2.fromBytes(bytes).getItems();
        
        assertEquals((Long) items.get(0), (Long) Long.MAX_VALUE);
        assertEquals((Long) items.get(1), (Long) ((long) 1));
        assertEquals((String) items.get(5), "foo");
        assertEquals((Float) items.get(8), (Float) ((float) -4.5));
        assertEquals((Boolean) items.get(9), new Boolean(true));
        assertEquals((BigInteger) items.get(10), new BigInteger("123456789123456789"));
        assertEquals((BigDecimal) items.get(12), new BigDecimal("-12345678912345.1234567891234"));
        assertEquals((Boolean) items.get(13), new Boolean(false));
    }

    @Test
    public void compareIntsAndBigInts() {
        // negative ints should always be less than positive BigInts
        Tuple2 tInt = new Tuple2();
        tInt = tInt.add(-1);
        Tuple2 tBigInt = new Tuple2();
        tBigInt = tBigInt.add(new BigInteger("1"));
        assertEquals(-1, tInt.compareTo(tBigInt));

        // positive ints should always be greater than negative BigInts
        tInt = new Tuple2();
        tInt = tInt.add(1);
        tBigInt = new Tuple2();
        tBigInt = tBigInt.add(new BigInteger("-1"));
        assertEquals(1, tInt.compareTo(tBigInt));
    }

    @Test
    public void bigDecOrdering() {
        Tuple2 t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("-1.29"));
        Tuple2 t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("-1.28"));
        assertEquals(-1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("1.28"));
        t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("1.27"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("1.27"));
        t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("-1.29"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigDecimal("1.28"));
        t2 = new Tuple2();
        t2 = t2.add(new BigDecimal("-1.27"));
        assertEquals(1, t1.compareTo(t2));
    }

    @Test
    public void bigIntOrdering() {
        Tuple2 t1 = new Tuple2();
        t1 = t1.add(new BigInteger("-129"));
        Tuple2 t2 = new Tuple2();
        t2 = t2.add(new BigInteger("-128"));
        assertEquals(-1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigInteger("128"));
        t2 = new Tuple2();
        t2 = t2.add(new BigInteger("127"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigInteger("127"));
        t2 = new Tuple2();
        t2 = t2.add(new BigInteger("-129"));
        assertEquals(1, t1.compareTo(t2));

        t1 = new Tuple2();
        t1 = t1.add(new BigInteger("128"));
        t2 = new Tuple2();
        t2 = t2.add(new BigInteger("-128"));
        assertEquals(1, t1.compareTo(t2));

        byte[] bytes = new byte[255];
        bytes[0] = Byte.MAX_VALUE;
        for (int i = 1; i < 255; i++) {
            bytes[i] = (byte) 0xff;
        }
        BigInteger bigInteger = new BigInteger(bytes);
        
        bytes = new byte[255];
        bytes[0] = Byte.MIN_VALUE;
        for (int i = 1; i < 255; i++) {
            bytes[i] = (byte) 0xff;
        }
        BigInteger negBigInteger = new BigInteger(bytes);
        
        t1 = new Tuple2();
        t1 = t1.add(bigInteger);
        
        t2 = new Tuple2();
        t2 = t2.add(negBigInteger);
        assertEquals(1, t1.compareTo(t2));
    }

    @Test
    public void bigBigIntegers() {
        byte[] bytes = new byte[255];
        bytes[0] = Byte.MAX_VALUE;
        for (int i = 1; i < 255; i++) {
            bytes[i] = (byte) 0xff;
        }
        BigInteger d87 = new BigInteger(bytes);

        BigInteger d86 = d87.subtract(BigInteger.ONE);
        BigInteger d88 = d87.add(BigInteger.ONE);

        Tuple2 t86 = Tuple2.from(d86);
        Tuple2 t87 = Tuple2.from(d87);
        Tuple2 t88 = Tuple2.from(d88);

        assertEquals(1, t87.compareTo(t86));
        assertEquals(-1, t87.compareTo(t88));
    }
}
