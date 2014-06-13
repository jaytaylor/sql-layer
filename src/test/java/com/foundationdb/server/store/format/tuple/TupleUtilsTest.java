package com.foundationdb.server.store.format.tuple;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

import com.foundationdb.server.store.format.tuple.TupleUtils.DecodeResult;

import static org.junit.Assert.*;

public class TupleUtilsTest {

	@Test
	public void doubleEncodingTest() {
		Double initDouble = 4.5;
		byte[] bytes = TupleUtils.encode(initDouble);
		DecodeResult result = TupleUtils.decodeDouble(bytes, 1);
		assertEquals(initDouble, (Double) result.o);
		
		initDouble = -4.5;
		bytes = TupleUtils.encode(initDouble);
		result = TupleUtils.decodeDouble(bytes, 1);
		assertEquals(initDouble, (Double) result.o);
		
		initDouble = 0.0;
		bytes = TupleUtils.encode(initDouble);
		result = TupleUtils.decodeDouble(bytes, 1);
		assertEquals(initDouble, (Double) result.o);
	}
	
	@Test
	public void floatEncodingTest() {
		Float initFloat = (float) 4.5;
		byte[] bytes = TupleUtils.encode(initFloat);
		DecodeResult result = TupleUtils.decodeFloat(bytes, 1);
		assertEquals(initFloat, (Float) result.o);
		
		initFloat = (float) -4.5;
		bytes = TupleUtils.encode(initFloat);
		result = TupleUtils.decodeFloat(bytes, 1);
		assertEquals(initFloat, (Float) result.o);
		
		initFloat = (float) 0.0;
		bytes = TupleUtils.encode(initFloat);
		result = TupleUtils.decodeFloat(bytes, 1);
		assertEquals(initFloat, (Float) result.o);
	}
	
	@Test
	public void bigIntEncodingTest() {
		BigInteger bigInteger = new BigInteger("12345678912345");
		byte[] bytes = TupleUtils.encode(bigInteger);
		DecodeResult result = TupleUtils.decodeBigInt(bytes, 1);
		assertEquals(bigInteger, (BigInteger) result.o);
		
		bigInteger = new BigInteger("-12345678912345");
		bytes = TupleUtils.encode(bigInteger);
		result = TupleUtils.decodeBigInt(bytes, 1);
		assertEquals(bigInteger, (BigInteger) result.o);
	}
	
	@Test
	public void bigDecEncodingTest() {
		BigDecimal bigDecimal = new BigDecimal("123456789.123456789");
		byte[] bytes = TupleUtils.encode(bigDecimal);
		DecodeResult result = TupleUtils.decodeBigDecimal(bytes, 1);
		assertEquals(bigDecimal, (BigDecimal) result.o);
		
		bigDecimal = new BigDecimal("-123456789.123456789");
		bytes = TupleUtils.encode(bigDecimal);
		result = TupleUtils.decodeBigDecimal(bytes, 1);
		assertEquals(bigDecimal, (BigDecimal) result.o);
	}
}
