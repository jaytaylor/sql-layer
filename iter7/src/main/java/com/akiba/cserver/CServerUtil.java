package com.akiba.cserver;

import java.io.File;
import java.lang.management.RuntimeMXBean;

public class CServerUtil {

	private final static boolean BIG_ENDIAN = false;

	private final static char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	public static long getSignedIntegerByWidth(final byte[] bytes,
			final int index, final int width) {
		switch (width) {
		case 0:
			return 0;
		case 1:
			return bytes[index];
		case 2:
			return getShort(bytes, index);
		case 3:
			return getMediumInt(bytes, index);
		case 4:
			return getInt(bytes, index);
		case 8:
			return getLong(bytes, index);
		default:
			throw new IllegalArgumentException(
					"Width must be 0,2,3,4 or 8 but is: " + width);
		}
	}

	public static long getUnsignedIntegerByWidth(final byte[] bytes,
			final int index, final int width) {
		switch (width) {
		case 0:
			return 0;
		case 1:
			return getByte(bytes, index) & 0xFF;
		case 2:
			return getShort(bytes, index) & 0xFFFF;
		case 3:
			return getMediumInt(bytes, index) & 0xFFFFFF;
		case 4:
			return getInt(bytes, index) & 0xFFFFFFFF;
		case 8:
			return getLong(bytes, index); // TODO
//			throw new UnsupportedOperationException(
//					"Currently can't handle unsigned 64-bit integers");
		default:
			throw new IllegalArgumentException(
					"Width must be 0,1,2,3,4 or 8 but is: " + width);
		}
	}

	public static int getByte(byte[] bytes, int index) {
		return (bytes[index + 0] & 0xFF);
	}

	public static int getShort(byte[] bytes, int index) {
		if (BIG_ENDIAN) {
			return (bytes[index + 1] & 0xFF) | (bytes[index + 0]) << 8;
		} else {
			return (bytes[index + 0] & 0xFF) | (bytes[index + 1]) << 8;
		}
	}

	public static int getChar(byte[] bytes, int index) {
		if (BIG_ENDIAN) {
			return (bytes[index + 1] & 0xFF) | (bytes[index + 0] & 0xFF) << 8;
		} else {
			return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8;
		}
	}

	public static int getMediumInt(byte[] bytes, int index) {
		if (BIG_ENDIAN) {
			return (bytes[index + 2] & 0xFF) | (bytes[index + 1] & 0xFF) << 8
					| (bytes[index + 0] & 0xFF) << 16;
		} else {
			return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8
					| (bytes[index + 2] & 0xFF) << 16;
		}
	}

	public static int getInt(byte[] bytes, int index) {
		if (BIG_ENDIAN) {
			return (bytes[index + 3] & 0xFF) | (bytes[index + 2] & 0xFF) << 8
					| (bytes[index + 1] & 0xFF) << 16
					| (bytes[index + 0] & 0xFF) << 24;
		} else {
			return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8
					| (bytes[index + 2] & 0xFF) << 16
					| (bytes[index + 3] & 0xFF) << 24;
		}
	}

	public static long getLong(byte[] bytes, int index) {
		if (BIG_ENDIAN) {
			return (long) (bytes[index + 7] & 0xFF)
					| (long) (bytes[index + 6] & 0xFF) << 8
					| (long) (bytes[index + 5] & 0xFF) << 16
					| (long) (bytes[index + 4] & 0xFF) << 24
					| (long) (bytes[index + 3] & 0xFF) << 32
					| (long) (bytes[index + 2] & 0xFF) << 40
					| (long) (bytes[index + 1] & 0xFF) << 48
					| (long) (bytes[index + 0] & 0xFF) << 56;
		} else {
			return (long) (bytes[index + 0] & 0xFF)
					| (long) (bytes[index + 1] & 0xFF) << 8
					| (long) (bytes[index + 2] & 0xFF) << 16
					| (long) (bytes[index + 3] & 0xFF) << 24
					| (long) (bytes[index + 4] & 0xFF) << 32
					| (long) (bytes[index + 5] & 0xFF) << 40
					| (long) (bytes[index + 6] & 0xFF) << 48
					| (long) (bytes[index + 7] & 0xFF) << 56;
		}
	}

	public static float getFloat(byte[] bytes, int index) {
		return Float.intBitsToFloat(getInt(bytes, index));
	}

	public static double getDouble(byte[] bytes, int index) {
		return Double.longBitsToDouble(getLong(bytes, index));
	}

	public static int putByte(byte[] bytes, int index, int value) {
		bytes[index] = (byte) (value);
		return index + 1;
	}

	public static int putShort(byte[] bytes, int index, int value) {
		if (BIG_ENDIAN) {
			bytes[index + 1] = (byte) (value);
			bytes[index + 0] = (byte) (value >>> 8);
		} else {
			bytes[index + 0] = (byte) (value);
			bytes[index + 1] = (byte) (value >>> 8);
		}
		return index + 2;
	}

	public static int putChar(byte[] bytes, int index, int value) {
		if (BIG_ENDIAN) {
			bytes[index + 1] = (byte) (value);
			bytes[index + 0] = (byte) (value >>> 8);
		} else {
			bytes[index + 0] = (byte) (value);
			bytes[index + 1] = (byte) (value >>> 8);
		}
		return index + 2;
	}

	public static int putMediumInt(byte[] bytes, int index, int value) {
		if (BIG_ENDIAN) {
			bytes[index + 2] = (byte) (value);
			bytes[index + 1] = (byte) (value >>> 8);
			bytes[index + 0] = (byte) (value >>> 16);
		} else {
			bytes[index + 0] = (byte) (value);
			bytes[index + 1] = (byte) (value >>> 8);
			bytes[index + 2] = (byte) (value >>> 16);
		}
		return index + 4;
	}

	public static int putInt(byte[] bytes, int index, int value) {
		if (BIG_ENDIAN) {
			bytes[index + 3] = (byte) (value);
			bytes[index + 2] = (byte) (value >>> 8);
			bytes[index + 1] = (byte) (value >>> 16);
			bytes[index + 0] = (byte) (value >>> 24);
		} else {
			bytes[index + 0] = (byte) (value);
			bytes[index + 1] = (byte) (value >>> 8);
			bytes[index + 2] = (byte) (value >>> 16);
			bytes[index + 3] = (byte) (value >>> 24);
		}
		return index + 4;
	}

	public static int putLong(byte[] bytes, int index, long value) {
		if (BIG_ENDIAN) {
			bytes[index + 7] = (byte) (value);
			bytes[index + 6] = (byte) (value >>> 8);
			bytes[index + 5] = (byte) (value >>> 16);
			bytes[index + 4] = (byte) (value >>> 24);
			bytes[index + 3] = (byte) (value >>> 32);
			bytes[index + 2] = (byte) (value >>> 40);
			bytes[index + 1] = (byte) (value >>> 48);
			bytes[index + 0] = (byte) (value >>> 56);
		} else {
			bytes[index + 0] = (byte) (value);
			bytes[index + 1] = (byte) (value >>> 8);
			bytes[index + 2] = (byte) (value >>> 16);
			bytes[index + 3] = (byte) (value >>> 24);
			bytes[index + 4] = (byte) (value >>> 32);
			bytes[index + 5] = (byte) (value >>> 40);
			bytes[index + 6] = (byte) (value >>> 48);
			bytes[index + 7] = (byte) (value >>> 56);
		}
		return index + 8;
	}

	public static int putFloat(byte[] bytes, int index, float value) {
		return putInt(bytes, index, Float.floatToIntBits(value));
	}

	public static int putDouble(byte[] bytes, int index, double value) {
		return putLong(bytes, index, Double.doubleToLongBits(value));
	}

	public static int putBytes(byte[] bytes, int index, byte[] value) {
		System.arraycopy(value, 0, bytes, index, value.length);
		return index + value.length;
	}

	public static int putBytes(byte[] bytes, int index, byte[] value,
			int offset, int length) {
		System.arraycopy(value, offset, bytes, index, length);
		return index + length;
	}

	public static String dump(char[] c, int offset, int size) {
		StringBuilder sb1 = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		for (int m = 0; m < size - offset; m += 8) {
			sb2.setLength(0);
			hex(sb1, m, 4);
			sb1.append(":");
			for (int i = 0; i < 8; i++) {
				sb1.append("  ");
				if (i % 4 == 0)
					sb1.append(" ");
				int j = m + i;
				if (j < size - offset) {
					hex(sb1, c[j + offset], 4);
					if (c[j + offset] >= 32 && c[j] < 127)
						sb2.append(c[j]);
					else
						sb2.append(".");
				} else
					sb1.append("    ");
			}
			sb1.append("    ");
			sb1.append(sb2.toString());
			sb1.append("\n");
		}
		return sb1.toString();
	}

	public static String dump(byte[] b, int offset, int size) {
		StringBuilder sb1 = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		for (int m = 0; m < size - offset; m += 16) {
			sb2.setLength(0);
			hex(sb1, m, 4);
			sb1.append(":");
			for (int i = 0; i < 16; i++) {
				sb1.append(" ");
				if (i % 8 == 0)
					sb1.append(" ");
				int j = m + i;
				if (j < size - offset) {
					hex(sb1, b[j + offset], 2);
					if (b[j + offset] >= 32 && b[j] < 127)
						sb2.append((char) b[j]);
					else
						sb2.append(".");
				} else
					sb1.append("  ");
			}
			sb1.append("  ");
			sb1.append(sb2.toString());
			sb1.append("\n");
		}
		return sb1.toString();
	}

	public static StringBuilder hex(StringBuilder sb, long value, int length) {
		for (int i = length - 1; i >= 0; i--) {
			sb.append(HEX_DIGITS[(int) (value >> (i * 4)) & 0xF]);
		}
		return sb;
	}
	
	public static StringBuilder hex(StringBuilder sb, byte[] bytes, int start, int length) {
		for (int i = start; i < start + length; i++) {
			sb.append(HEX_DIGITS[(bytes[i] & 0xF0) >>> 4]);
			sb.append(HEX_DIGITS[(bytes[i] & 0xF0)]);
		}
		return sb;
	}

	public static void printRuntimeInfo() {
		System.out.println();
		RuntimeMXBean m = java.lang.management.ManagementFactory
				.getRuntimeMXBean();
		System.out.println("BootClassPath = " + m.getBootClassPath());
		System.out.println("ClassPath = " + m.getClassPath());
		System.out.println("LibraryPath = " + m.getLibraryPath());
		System.out.println("ManagementSpecVersion = "
				+ m.getManagementSpecVersion());
		System.out.println("Name = " + m.getName());
		System.out.println("SpecName = " + m.getSpecName());
		System.out.println("SpecVendor = " + m.getSpecVendor());
		System.out.println("SpecVersion = " + m.getSpecVersion());
		System.out.println("UpTime = " + m.getUptime());
		System.out.println("VmName = " + m.getVmName());
		System.out.println("VmVendor = " + m.getVmVendor());
		System.out.println("VmVersion = " + m.getVmVersion());
		System.out.println("InputArguments = " + m.getInputArguments());
		System.out.println("BootClassPathSupported = "
				+ m.isBootClassPathSupported());
		System.out.println("---all properties--");
		System.out.println("SystemProperties = " + m.getSystemProperties());
		System.out.println("---");
		System.out.println();

	}

	public final static void cleanUpDirectory(final File file) {
		if (!file.exists()) {
			file.mkdirs();
			return;
		} else if (file.isFile()) {
			throw new IllegalStateException(file + " must be a directory");
		} else {
			final File[] files = file.listFiles();
			cleanUpFiles(files);
		}
	}

	public final static void cleanUpFiles(final File[] files) {
		for (final File file : files) {
			if (file.isDirectory()) {
				cleanUpDirectory(file);
			}
			file.delete();
		}
	}

}
