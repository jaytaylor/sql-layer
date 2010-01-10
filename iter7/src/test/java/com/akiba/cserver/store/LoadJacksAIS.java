package com.akiba.cserver.store;

import java.io.StringReader;
import java.util.Properties;

import com.persistit.Persistit;
import com.persistit.StreamSaver;

public class LoadJacksAIS {
	private final static String N = Persistit.NEW_LINE;

	private final static String PROPERTIES = "datapath =src/test/resources"
			+ N
			+ "buffer.count.8192 = 4K"
			+ N
			+ "volume.1 = ${datapath}/ais_txn.v00,create,pageSize:8K,initialSize:1M,extensionSize:1M,maximumSize:10G"
			+ N
			+ "volume.2 = ${datapath}/ais_data.v00,create,pageSize:8k,initialSize:5M,extensionSize:5M,maximumSize:100G"
			+ N
			+ "volume.3 = ${datapath}/ais_system.v00,create,pageSize:8k,initialSize:5M,extensionSize:5M,maximumSize:100G"
			+ N + "pwjpath  = /tmp/persistit.pwj" + N + "pwjsize  = 8M" + N
			+ "pwjdelete = true" + N + "pwjcount = 2" + N + "jmx = false" + N
			+ "showgui = true" + N;

	public static void main(final String[] args) throws Exception {
		Persistit db = new Persistit();
		final Properties properties = new Properties();
		properties.load(new StringReader(PROPERTIES));
		db.initialize(properties);
		StreamSaver saver = new StreamSaver(db, "src/test/resources/ais2.sav");
		saver.saveTrees("ais_data", new String[] { "ais" });
		saver.close();
		db.close();
	}
}
