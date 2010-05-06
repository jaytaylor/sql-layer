package com.akiban.cserver;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Loosely inspired by systemtap, this class implements a generic mechanism for
 * timing and counting executions of code sections. Application code should do
 * the following:
 * 
 * <pre>
 *   static Tap MY_TAP = Tap.add("myTap");
 *   ...
 *   MY_TAP.in();
 *   ... code section to measure
 *   MY_TAP.out();
 * </pre>
 * <p />
 * The static {@link #add(String)} method creates an instance of a
 * {@link Dispatch} initially set to {@link Null}. HotSpot optimizes out the
 * calls to MY_TAP.in() and MY_TAP.out() so these lines of code do not affect
 * performance.
 * <p />
 * When needed, call {@link #setEnabled(String, boolean)} to turn on/off
 * performance monitoring for one or more Tap objects by name. The String
 * argument is a regEx expression applied to the name; for example
 * <code>Tap.setEnabled("myTap", true)</code> will enable monitoring of the code
 * section in the example above.
 * <p />
 * The results are available in String or object form. Call {@link #report()} to
 * get a printable string representation of the results. Use MY_TAP.getResult()
 * to get a Result object.
 * <p />
 * Note that the TimeAndCount subclass of {@link Tap} uses {@link
 * System.nanoTime()} to measure elapsed time. That call takes several hundred
 * nanoseconds, so don't attempt to use this class to measure very short code
 * segments.
 * <p />
 * The {@link #in()} and {@link #out()} methods are meant to be paired, but it
 * is not necessary to implement a try/finally block to guarantee that
 * {@link #out()} is called. Generally this makes the tool easier to use, but it
 * means there is no validation that every call to {@link #in()} is matched with
 * a corresponding {@link #out()}. (However, {@link #out()} will throw a
 * RuntimeException if not preceded by {@link #in()}.)
 * 
 * @author peter
 * 
 */
public abstract class Tap {

	private static Map<String, Dispatch> dispatches = new TreeMap<String, Dispatch>();

	public static Dispatch add(final String name) {
		final Dispatch dispatch = new Dispatch(name);
		dispatches.put(name, dispatch);
		return dispatch;
	}

	public static Dispatch remove(final String name) {
		return dispatches.remove(name);
	}

	public static void setEnabled(final String regExPattern, final boolean on) {
		final Pattern pattern = Pattern.compile(regExPattern);
		for (final Dispatch tap : dispatches.values()) {
			if (pattern.matcher(tap.getName()).matches()) {
				tap.setEnabled(on);
			}
		}
	}

	public static String report() {
		final StringBuilder sb = new StringBuilder();
		for (final Tap tap : dispatches.values()) {
			tap.appendReport(sb);
		}
		return sb.toString();
	}

	protected final String name;

	public Tap(final String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	/**
	 * Mark the beginning of a section of code to be timed
	 */
	public abstract void in();

	/**
	 * Mark the end of a section of code to be timed
	 */
	public abstract void out();

	/**
	 * Reset any accumulated statistics to zero.
	 */
	public abstract void reset();

	/**
	 * Append a line of text to the supplied {@link StringBuilder} containing
	 * the current accumulated statistics for this Tap. The Null Tap should do
	 * nothing.
	 * 
	 * @param sb
	 */
	public abstract void appendReport(final StringBuilder sb);

	/**
	 * Return a {@link Result} object containing the accumulated statistics for
	 * this Tap. The Null Tap should return null.
	 * 
	 * @return A Result object or <tt>null</tt>
	 */
	public abstract Result getResult();

	/**
	 * Implementation that simply dispatches to another Tap instance. Used to
	 * hold the "switch" that determines whether a Null, TimeAndCount or other
	 * kind of Tap is invoked.
	 * 
	 * Reason for this dispatch mechanism is that HotSpot seems to be able to
	 * optimize out a dispatch to an instance of Null. Therefore code can invoke
	 * the methods of a Dispatch object set to Null with low or no performance
	 * penalty.
	 * 
	 */
	private static class Dispatch extends Tap {

		protected Tap tap;

		/**
		 * Base class dispatched to either NullTap or TimerTap.
		 * 
		 * @param name
		 */
		public Dispatch(final String name) {
			super(name);
			this.tap = new Null(name);
		}

		public final void setEnabled(final boolean on) {
			tap = on ? new TimeAndCount(name) : new Null(name);
		}

		public void in() {
			tap.in();
		}

		public void out() {
			tap.out();
		}

		public void reset() {
			tap.reset();
		}

		public String getName() {
			return name;
		}

		public void appendReport(final StringBuilder sb) {
			tap.appendReport(sb);
		}

		public Result getResult() {
			return tap.getResult();
		}

		public String toString() {
			return tap.toString();
		}
	}

	public static class Null extends Tap {

		public Null(final String name) {
			super(name);
		}

		public void in() {
			// do nothing
		}

		public void out() {
			// do nothing
		}

		public void reset() {
			// do nothing
		}

		public void appendReport(final StringBuilder sb) {
			// do nothing;
		}

		public String toString() {
			return "NullTap(" + name + ")";
		}

		public Result getResult() {
			return null;
		}
	}

	/**
	 * A Tap subclass that counts and times in/out calls.
	 * 
	 * @author peter
	 * 
	 */
	public static class TimeAndCount extends Tap {

		public TimeAndCount(final String name) {
			super(name);
		}

		long cumulativeNanos = 0;
		long cumulativeCount = 0;
		long inNanos = Long.MIN_VALUE;

		public void in() {
			inNanos = System.nanoTime();
		}

		public void out() {
			if (inNanos == Long.MIN_VALUE) {
				throw new RuntimeException("TimerTap out() before in()");
			}
			long now = System.nanoTime();
			cumulativeNanos += now - inNanos;
			cumulativeCount++;
			inNanos = Long.MIN_VALUE;
		}

		public void reset() {
			cumulativeCount = 0;
			cumulativeNanos = 0;
			inNanos = Long.MIN_VALUE;
		}

		public void appendReport(final StringBuilder sb) {
			sb.append(String.format("%20s count=%,10d time=%,12dms", name,
					cumulativeCount, cumulativeNanos / 1000000));
			if (cumulativeCount > 0) {
				sb.append(String.format(" per=%,10dns", cumulativeNanos
						/ cumulativeCount));
			}
			sb.append(CServerUtil.NEW_LINE);
		}

		public String toString() {
			return String.format("%s count=%,d time=%,dms", name,
					cumulativeCount, cumulativeNanos / 1000000);
		}

		public Result getResult() {
			return new Result(getName(), cumulativeCount, cumulativeNanos);
		}

	}

	public static class Result {

		private final String name;

		private final long cumulativeCount;

		private final long cumulativeTime;

		public Result(final String name, final long cumulativeCount,
				final long cumulativeTime) {
			this.name = name;
			this.cumulativeCount = cumulativeCount;
			this.cumulativeTime = cumulativeTime;
		}

		public String getName() {
			return name;
		}

		public long getCumulativeCount() {
			return cumulativeCount;
		}

		public long getCumulativeTime() {
			return cumulativeTime;
		}
	}

	public static void main(final String[] args) {
		// A microbenchmark to prove a Dispatch to Null gets optimized
		// away. Actually, the results are strange: the 
		final Dispatch t1 = add("t1");
		final Dispatch t2 = add("t2");
		t1.setEnabled(true);

		long z = 0;

		long count = 1000 * 1000 * 1000;

		{
			System.out.print("Loop " + count + " times, Null Tap invoked: ");
			t1.in();
			long t = 0;
			for (long i = 0; i < count; i++) {
				t2.in();
				t = t + i;
				if (t > count) {
					t = t / 2;
				}
				t2.out();
			}
			t1.out();
			System.out.println("  " + t1.toString());
			t1.reset();
			z += t;
		}

		{
			System.out.print("Loop " + count + " times, no Tap");
			t1.in();
			long t = 0;
			for (long i = 0; i < count; i++) {
				t = t + i;
				if (t > count) {
					t = t / 2;
				}
			}
			t1.out();
			System.out.println("  " + t1.toString());
			t1.reset();
			z += t;
		}

		count = count / 100;
		
		{
			System.out.print("Loop " + count
					+ " times, TimeAndCount Tap invoked: ");
			t2.setEnabled(true);
			t1.in();
			long t = 0;
			for (long i = 0; i < count; i++) {
				t2.in();
				t = t + i;
				if (t > count) {
					t = t / 2;
				}
				t2.out();
			}
			t1.out();
			System.out.println("  " + t1.toString());
			t1.reset();
			z += t;
		}
		
		System.out.println("Report:");
		System.out.println(report());

		System.out.println("Printing a computed result to make sure "
				+ "HotSpot doesn't optimize the whole computation away: " + z);

	}
}
