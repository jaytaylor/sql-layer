package com.foundationdb.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.nio.charset.Charset;

import com.foundationdb.tuple.*;

/**
 * 
 * Utility functions for encoding/decoding tuples.
 *
 */
class TupleUtils {
	private static final byte nil = 0x0;
	private static final byte[] nil_rep = new byte[] {nil, (byte)0xFF};
	private static final BigInteger[] size_limits;
	private static final Charset UTF8;
	
	static final int FLOAT_LEN = 4;
	static final int DOUBLE_LEN = 8;
	static final int INT_LEN = 4;

	static final byte FLOAT_CODE = 0x20;
	static final byte DOUBLE_CODE = 0x21;
	static final byte BIGDEC_CODE = 0x1d;

	static {
		size_limits = new BigInteger[9];
		for(int i = 0; i < 9; i++) {
			size_limits[i] = (BigInteger.ONE).shiftLeft(i * 8).subtract(BigInteger.ONE);
		}
		UTF8 = Charset.forName("UTF-8");
	}

	static class DecodeResult {
		final int end;
		final Object o;

		DecodeResult(int pos, Object o) {
			this.end = pos;
			this.o = o;
		}
	}

	public static byte[] join(List<byte[]> items) {
		return ByteArrayUtil.join(null, items);
	}

	static byte[] floatingPointToByteArray (float value) {
		return ByteBuffer.allocate(FLOAT_LEN).putFloat(value).order(ByteOrder.BIG_ENDIAN).array();
	}

	static byte[] floatingPointToByteArray(double value) {
		return ByteBuffer.allocate(DOUBLE_LEN).putDouble(value).order(ByteOrder.BIG_ENDIAN).array();
	}

	static float byteArrayToFloat(byte[] bytes) {
		return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getFloat();
	}

	static double byteArrayToDouble(byte[] bytes) {
		return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
	}
	
	/**
	 * If the sign bit is 1, flips all bits in the {@code byte[]}
	 * Else, just flips the sign bit.
	 * 
	 * @param bytes - a Big-Endian IEEE binary representation of float, double, or BigInteger
	 * @return the encoded {@code byte[]}
	 */
	static byte[] floatingPointEncoding(byte[] bytes) {
		if ((bytes[0] & (byte) 0x80) != (byte) 0x00) {
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = (byte) (bytes[i] ^ 0xff);
			}
		}
		else {
			bytes[0] = (byte) (0x80 ^ bytes[0]); 
		}

		return bytes;
	}

	/**
	 * If the sign bit is 0, flips all bits in the {@code byte[]}
	 * Else, just flips the sign bit.
	 * 
	 * @param bytes - an encoded Big-Endian IEEE binary representation of float, double, or BigInteger
	 * @return the decoded {@code byte[]}
	 */
	static byte[] floatingPointDecoding(byte[] bytes) {
		if ((bytes[0] & (byte) 0x80) != (byte) 0x80) {
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = (byte) (bytes[i] ^ 0xff);
			}
		}
		else {
			bytes[0] = (byte) (0x80 ^ bytes[0]); 
		}

		return bytes;
	}
	
	static byte[] encode(Object t) {
		if(t == null)
			return new byte[] {nil};
		if(t instanceof byte[])
			return encode((byte[]) t);
		if(t instanceof String)
			return encode((String) t);
		if (t instanceof Float)
			return encode((Float) t);
		if (t instanceof Double)
			return encode((Double) t);
		if (t instanceof BigDecimal)
			return encode((BigDecimal) t);
		if (t instanceof Number) 
			return encode(((Number)t).longValue());
		throw new IllegalArgumentException("Unsupported data type: " + t.getClass().getName());
	}

	static byte[] encode(byte[] bytes) {
		List<byte[]> list = new ArrayList<byte[]>(3);
		list.add(new byte[] {0x1});
		list.add(ByteArrayUtil.replace(bytes, new byte[] {0x0}, nil_rep));
		list.add(new byte[] {0x0});

		//System.out.println("Joining bytes...");
		return ByteArrayUtil.join(null, list);
	}

	static byte[] encode(String s) {
		List<byte[]> list = new ArrayList<byte[]>(3);
		list.add(new byte[] {0x2});
		list.add(ByteArrayUtil.replace(s.getBytes(UTF8), new byte[] {0x0}, nil_rep));
		list.add(new byte[] {0x0});

		//System.out.println("Joining string...");
		return ByteArrayUtil.join(null, list);
	}

	static byte[] encode(long i) {
		//System.out.println("Encoding integral " + i);
		if(i == 0) {
			return new byte[] { 20 };
		}
		if(i > 0) {
			int n = ByteArrayUtils.bisectLeft(size_limits, BigInteger.valueOf(i));
			assert n <= size_limits.length;
			byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(i).array();
			//System.out.println("  -- integral has 'n' of " + n + " and output bytes of " + bytes.length);
			byte[] result = new byte[n+1];
			result[0] = (byte)(20 + n);
			System.arraycopy(bytes, bytes.length - n, result, 1, n);
			return result;
		}
		BigInteger bI = BigInteger.valueOf(i);
		int n = ByteArrayUtils.bisectLeft(size_limits, bI.negate());

		assert n >= 0 && n < size_limits.length; // can we do this? it seems to be required for the following statement

		long maxv = size_limits[n].add(bI).longValue();
		byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(maxv).array();
		byte[] result = new byte[n+1];
		result[0] = (byte)(20 - n);
		System.arraycopy(bytes, bytes.length - n, result, 1, n);
		return result;
	}
	
	public static byte[] encode(Float value) {
		byte[] bytes = floatingPointToByteArray(value);
		bytes = floatingPointEncoding(bytes);	
		byte[] typecode = {FLOAT_CODE};
		return ByteArrayUtil.join(typecode, bytes);
	}

	public static byte[] encode(Double value) {
		byte[] bytes = floatingPointToByteArray(value);
		bytes = floatingPointEncoding(bytes);
		byte[] typecode =  {DOUBLE_CODE};
		return ByteArrayUtil.join(typecode, bytes);
	}

	public static Float decodeFloat(byte[] bytes) {
		bytes = floatingPointDecoding(Arrays.copyOfRange(bytes, 0, bytes.length));
		return byteArrayToFloat(bytes);
	}

	public static Double decodeDouble(byte[] bytes) {
		bytes = floatingPointDecoding(Arrays.copyOfRange(bytes, 0, bytes.length));
		return byteArrayToDouble(bytes);
	}

	static byte[] encode(Integer i) {
		return encode(i.longValue());
	}
	
	public static byte[] encode(BigDecimal value) {
		byte[] bigIntBytes = encodeBigInt(value.unscaledValue());
		byte[] scaleBytes = encodeInt(value.scale());
		byte[] typecode = {BIGDEC_CODE}; 
		byte[] length = encodeInt(bigIntBytes.length);
		return ByteArrayUtil.join(typecode, scaleBytes, length, bigIntBytes);
	}
	
	public static DecodeResult decodeBigDecimal(byte[] bytes, int start) {
		int scale = decodeInt(Arrays.copyOfRange(bytes, start, start + INT_LEN));
		int length = decodeInt(Arrays.copyOfRange(bytes, start + INT_LEN, start + INT_LEN * 2));
		BigInteger bigInt = decodeBigInt(Arrays.copyOfRange(bytes, start + INT_LEN * 2, start + INT_LEN * 2 + length));
		return new DecodeResult(start + INT_LEN * 2 + length, new BigDecimal(bigInt, scale));
	}
	
	static byte[] encodeBigInt(BigInteger value) {
		byte[] bytes = value.toByteArray();
		return floatingPointEncoding(bytes);
	}

	static BigInteger decodeBigInt(byte[] bytes) {
		bytes = floatingPointDecoding(bytes);
		return new BigInteger(bytes);
	}

	static byte[] encodeInt(int i) {
		return ByteBuffer.allocate(INT_LEN).order(ByteOrder.LITTLE_ENDIAN).putInt(i).array();
	}

	public static int decodeInt(byte[] bytes) {
		if(bytes.length != INT_LEN) {
			throw new IllegalArgumentException("Source array must be of length "+String.valueOf((INT_LEN)));
		}
		return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
	}

	static DecodeResult decode(byte[] rep, int pos, int last) {
		//System.out.println("Decoding '" + ArrayUtils.printable(rep) + "' at " + pos);

		// SOMEDAY: codes over 127 will be a problem with the signed Java byte mess
		int code = rep[pos];
		int start = pos + 1;
		if(code == 0x0) {
			return new DecodeResult(start, null);
		}
		if(code == 0x1) {
			int end = ByteArrayUtils.findTerminator(rep, (byte)0x0, (byte)0xff, start, last);
			//System.out.println("End of byte string: " + end);
			byte[] range = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			//System.out.println(" -> byte string contents: '" + ArrayUtils.printable(range) + "'");
			return new DecodeResult(end + 1, range);
		}
		if(code == 0x2) {
			int end = ByteArrayUtils.findTerminator(rep, (byte)0x0, (byte)0xff, start, last);
			//System.out.println("End of UTF8 string: " + end);
			byte[] stringBytes = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			String str = new String(stringBytes, UTF8);
			//System.out.println(" -> UTF8 string contents: '" + str + "'");
			return new DecodeResult(end + 1, str);
		}
		if (code == FLOAT_CODE) {
			int end = start + FLOAT_LEN;
			byte[] range = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			return new DecodeResult(end, decodeFloat(range));
		}
		if (code == DOUBLE_CODE) {
			int end = start + DOUBLE_LEN;
			byte[] range = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			return new DecodeResult(end, decodeDouble(range));
		}
		if (code == BIGDEC_CODE) {
			// have to calculate end within the function, since the length is variable / determined in the function.
			return decodeBigDecimal(rep, start); // TODO: fix inconsistency maybe?
		}
		if(code >=12 && code <=28) {
			// decode a long
			byte[] longBytes = new byte[9];
			Arrays.fill(longBytes, (byte)0);
			boolean upper = code >= 20;
			int n = upper ? code - 20 : 20 - code;
			int end = start + n;

			if(rep.length < end) {
				throw new RuntimeException("Invalid tuple (possible truncation)");
			}

			System.arraycopy(rep, start, longBytes, 9-n, n);
			if (!upper)
				for(int i=9-n; i<9; i++)
					longBytes[i] = (byte)~longBytes[i];

			BigInteger val = new BigInteger(longBytes);
			if (!upper) val = val.negate();

			if (val.compareTo(BigInteger.valueOf(Long.MIN_VALUE))<0 ||
				val.compareTo(BigInteger.valueOf(Long.MAX_VALUE))>0)
				throw new RuntimeException("Value out of range for type long.");

			return new DecodeResult(end, val.longValue());
		}
		throw new IllegalArgumentException("Unknown tuple data type " + code + " at index " + pos);
	}

	static List<Object> unpack(byte[] bytes, int start, int length) {
		List<Object> items = new LinkedList<Object>();
		int pos = start;
		int end = start + length;
		while(pos < bytes.length) {
			DecodeResult decoded = decode(bytes, pos, end);
			items.add(decoded.o);
			pos = decoded.end;
		}
		return items;
	}

	static byte[] pack(List<Object> items) {
		if(items.size() == 0)
	        return new byte[0];

		List<byte[]> parts = new ArrayList<byte[]>(items.size());
		for(Object t : items) {
			//System.out.println("Starting encode: " + ArrayUtils.printable((byte[])t));
			byte[] encoded = encode(t);
			//System.out.println(" encoded -> '" + ArrayUtils.printable(encoded) + "'");
			parts.add(encoded);
		}
		//System.out.println("Joining whole tuple...");
		return ByteArrayUtil.join(null, parts);
	}

	public static void main(String[] args) {
		try {
			byte[] bytes = encode( 4 );
			assert 4 == (Integer)(decode( bytes, 0, bytes.length ).o);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}

		try {
			byte[] bytes = encode( "\u021Aest \u0218tring" );
			String string = (String)(decode( bytes, 0, bytes.length ).o);
			System.out.println("contents -> " + string);
			assert "\u021Aest \u0218tring" == string;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}
		
		try {
			byte[] bytes = encode(4.5);
			Double result = (Double)(decode(bytes, 0, bytes.length).o);
			assert result == 4.5;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}
		
		try {
			byte[] bytes = encode(-4.5);
			Double result = (Double)(decode(bytes, 0, bytes.length).o);
			assert result == -4.5;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}

		try {
			BigDecimal test = new BigDecimal("123456789.123456789");
			byte[] bytes = encode(test);
			BigDecimal result = (BigDecimal)(decode(bytes, 0, bytes.length).o);
			assert result == test;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}
		
		/*Object[] a = new Object[] { "\u0000a", -2, "b\u0001", 12345, ""};
		List<Object> o = Arrays.asList(a);
		byte[] packed = pack( o );
		System.out.println("packed length: " + packed.length);
		o = unpack( packed );
		System.out.println("unpacked elements: " + packed);
		for(Object obj : o)
			System.out.println(" -> type: " + obj.getClass().getName());*/
	}
	
	private TupleUtils() {}
}










