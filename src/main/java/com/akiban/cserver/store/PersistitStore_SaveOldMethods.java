package com.akiban.cserver.store;

import java.io.StringReader;
import java.util.Properties;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.message.WriteRowResponse;
import com.akiban.message.AkibaConnection;
import com.akiban.message.Message;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Volume;
import com.persistit.exception.TreeNotFoundException;

public class PersistitStore_SaveOldMethods {

	private final RowDefCache rowDefCache = new RowDefCache();

	public final static int OK = 1;
	public final static int END = 2;
	public final static int ERR = 100;

	private final static String N = Persistit.NEW_LINE;

	private final static String PROPERTIES = "datapath = ."
			+ N
			+ "logpath = ${datapath}"
			+ N
			+ "#rmiport = 1099"
			+ N
			+ "logfile = ${logpath}/persistit_${timestamp}.log"
			+ N
			+ "verbose = true"
			+ N
			+ "buffer.count.8192 = 32K"
			+ N
			+ "volume.1 = ${datapath}/sys_txn.v0,create,pageSize:8K,initialSize:1M,extensionSize:1M,maximumSize:10G"
			+ N
			+ "volume.2 = ${datapath}/aktest.v01,create,pageSize:8k,initialSize:5M,extensionSize:5M,maximumSize:100G"
			+ N + "pwjpath  = ${datapath}/persistit.pwj" + N + "pwjsize  = 8M"
			+ N + "pwjdelete = true" + N + "pwjcount = 2" + N + "jmx = false"
			+ N + "showgui = false" + N;

	private static String datapath;

	private static Persistit db;

	private static long startTime;

	private static ThreadLocal<Exchange> exchangeLocal = new ThreadLocal<Exchange>();

	public static void setDataPath(final String path) {
		datapath = path;
	}

	public synchronized void startUp() throws Exception {

		//Util.printRuntimeInfo();

		if (db == null) {
			final Properties properties = new Properties();
			properties.load(new StringReader(PROPERTIES));
			db = new Persistit();
			db.setProperty("datapath", datapath);
			final long t = System.currentTimeMillis();
			db.initialize(properties);
			System.err.println("Persistit startup complete at: "
					+ db.elapsedTime() + "ms - took ("
					+ (System.currentTimeMillis() - t) + "ms)");
			System.err.flush();
			startTime = System.currentTimeMillis();
		}
	}

	public synchronized void shutDown() throws Exception {
		if (db != null) {
			final long t = System.currentTimeMillis();
			db.close();
			System.err.println("Persistit shutDown complete at: "
					+ db.elapsedTime() + "ms - took "
					+ (System.currentTimeMillis() - t) + "ms");
			System.err.println("Persitit was up for "
					+ (System.currentTimeMillis() - startTime) + "ms)");
			System.err.flush();
			db = null;
		}
	}

	// --------------------- Store interface --------------------

	public void writeRow(final AkibaConnection connection, final RowData rowData)
			throws Exception {
		final int result = writeRow(rowData);
		final Message message = new WriteRowResponse(result);
		connection.send(message);
	}

	// ----------------------------------------------------------

	public int createDatabase(final String dbName) throws Exception {
		try {
			db.loadVolume("${datapath}/" + dbName + ",create,pageSize:8k,"
					+ "initialSize:10M,extensionSize:10M,maximumSize:100G");
			return OK;
		} catch (Throwable e) {
			e.printStackTrace();
			return ERR;
		}
	}

	public static int dropDatabase(final String dbName) throws Exception {

		final Volume volume = db.getVolume(dbName);
		if (volume != null) {
			try {
				db.deleteVolume(dbName);
				return OK;
			} catch (Exception e) {
				e.printStackTrace();
				return ERR;
			}
		} else {
			return OK;
		}
	}

	/**
	 * Tests the existence of a database by name (For unit test)
	 * 
	 * @param dbName
	 * @return
	 * @throws Exception
	 */
	public static boolean exists(final String dbName) throws Exception {
		final Volume volume = db.getVolume(dbName);
		return volume != null;
	}

	public static int createTable(final String dbName, final String tableName)
			throws Exception {
		try {

			final Volume volume = db.getVolume(dbName);
			if (volume == null) {
				return ERR;
			} else {
				volume.getTree(tableName, true);
				return OK;
			}
		} catch (Throwable t) {
			System.err.println("PersistitStore.createTable "
					+ "LOG: throwable exception recieved");
			t.printStackTrace();
			return ERR;
		}
	}

	public int openTable(final String dbName, final String tableName)
			throws Exception {
		final Volume volume = db.getVolume(dbName);
		if (volume == null) {
			return ERR;
		}
		try {
			Exchange oldExchange = exchangeLocal.get();
			if (oldExchange != null) {
				db.releaseExchange(oldExchange);
			}
			Exchange newExchange = db.getExchange(volume, tableName, false);
			exchangeLocal.set(newExchange);
			return OK;
		} catch (TreeNotFoundException tnfe) {
			return ERR;
		}
	}

	public int closeTable(final String dbName, final String tableName)
			throws Exception {

		final Exchange exchange = exchangeLocal.get();
		if (exchange != null) {
			db.releaseExchange(exchange);
			exchangeLocal.set(null);
		}
		return OK;
	}

	public int dropTable(final String dbName, final String tableName)
			throws Exception {
		final Volume volume = db.getVolume("aktest");
		if (volume == null) {
			return OK;
		} else {
			volume.removeTree(tableName);
			return OK;
		}
	}

	private int writeRow(final RowData rowData) {
		final Exchange exchange = exchangeLocal.get();
		if (exchange == null) {
			return ERR;
		}
		final Transaction transaction = db.getTransaction();
		transaction.begin();
		try {
			exchange.clear().append("h");
			long pk = exchange.incrementValue(1, 1);
			exchange.append(pk);
			
			final int start = rowData.getInnerStart();
			final int size = rowData.getInnerSize();
			
			System.arraycopy(rowData.getBytes(), start,
					exchange.getValue().getEncodedBytes(), 0, size);
			exchange.getValue().setEncodedSize(size);
			
			exchange.store();
			transaction.commit();
			return OK;
		} catch (Throwable t) {
			t.printStackTrace();
			return ERR;
		}

		finally {
			transaction.end();
		}
	}

	
	public int updateRow(final RowData rowData, final byte[] fieldMap)
			throws Exception {
		throw new UnsupportedOperationException();
	}

	public int scanTable() throws Exception {
		final Exchange exchange = exchangeLocal.get();
		if (exchange == null) {
			return ERR;
		}
		exchange.clear().append("h").append(0);
		return OK;
	}

	public int scanNextRow(RowData rowData) throws Exception {
		final Exchange exchange = exchangeLocal.get();
		if (exchange == null) {
			return ERR;
		}

		if (exchange.next()) {
			// marshaller.valueToBb(exchange.getValue(), rowData);
			return OK;
		} else {
			return END;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Chunky#exists(java.lang.String, java.lang.String)
	 */
	public static boolean exists(final String dbName, final String tableName)
			throws Exception {
		final Volume volume = db.getVolume("aktest");
		return volume == null ? false
				: volume.getTree(tableName, false) != null;
	}

	public static Exchange getOpenTableExchange() {
		return exchangeLocal.get();
	}

}
