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
		
		bytes = TupleFloatingUtil.encode((double) -42);
		assert ByteArrayUtil.printable(bytes) == "=\\xd7\\xff\\xff";
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
}
