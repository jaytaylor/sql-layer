package com.foundationdb.server.store.format.tuple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.foundationdb.tuple.ByteArrayUtil;

import static org.junit.Assert.*;

public class TuplesTest {

	@Test
	public void tuplesTest() {
		
		Tuples t = new Tuples();
		t = t.add(Long.MAX_VALUE);
		t = t.add(1);
		t = t.add(0);
		t = t.add(-1);
		t = t.add(Long.MIN_VALUE);
		t = t.add("foo");
		t = t.add(4.5);
		t = t.add((Float) (float) 4.5);
		t = t.add((Float) (float) -4.5);
		t = t.add(new BigInteger("123456789123456789"));
		t = t.add(new BigDecimal("123456789.123456789"));
		t = t.add(new BigDecimal("-12345678912345.1234567891234"));
		byte[] bytes = t.pack();
		List<Object> items = Tuples.fromBytes(bytes).getItems();
		
		assertEquals((Long) items.get(0), (Long) Long.MAX_VALUE);
		assertEquals((Long) items.get(1), (Long) ((long) 1));
		assertEquals((String) items.get(5), "foo");
		assertEquals((Float) items.get(8), (Float) ((float) -4.5));
		assertEquals((BigInteger) items.get(9), new BigInteger("123456789123456789"));
		assertEquals((BigDecimal) items.get(11), new BigDecimal("-12345678912345.1234567891234"));
	}
	
}
