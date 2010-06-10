package com.akiban.cserver;

import java.io.File;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Source;
import com.akiban.ais.model.Target;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.Store;

public class CServerAisSourceTest extends TestCase implements CServerConstants {

	private final static File DATA_PATH = new File("/tmp/data");

	private final static String DDL_FILE_NAME = "src/test/resources/data_dictionary_test.ddl";

	private Store store;

	private AkibaInformationSchema ais;

	@Override
	public void setUp() throws Exception {
		store = new PersistitStore(CServerConfig.unitTestConfig(), new RowDefCache());
		CServerUtil.cleanUpDirectory(DATA_PATH);
		PersistitStore.setDataPath(DATA_PATH.getPath());
		store.startUp();
		store.setVerbose(true);
		store.getRowDefCache().setAIS(new CServer(false).primordialAIS());
		this.ais = new DDLSource().buildAIS(DDL_FILE_NAME);
	}

	@Override
	public void tearDown() throws Exception {
		store.shutDown();
		store = null;
	}

	public void testCServerAis() throws Exception {
		// Store AIS data in Chunk Server
		final Target target = new CServerAisTarget(store);
		new Writer(target).save(ais);
		
		// Retrieve AIS data from Chunk Server
		final Source source = new CServerAisSource(store);
		final AkibaInformationSchema aisCopy = new Reader(source).load();
		
//		new Writer(new SqlTextTarget(new PrintWriter(new FileWriter("/tmp/ais1.txt")))).save(ais);
//		new Writer(new SqlTextTarget(new PrintWriter(new FileWriter("/tmp/ais2.txt")))).save(aisCopy);

		assertTrue(equals(ais, aisCopy));
	}
	
	public void testReloadAIS() throws Exception {
		// Store AIS data in Chunk Server
		final Target target = new CServerAisTarget(store);
		new Writer(target).save(ais);
		
		// Retrieve AIS data from Chunk Server
		final Source source1 = new CServerAisSource(store);
		final AkibaInformationSchema aisCopy1 = new Reader(source1).load();
		new Writer(target).save(aisCopy1);

		final Source source2 = new CServerAisSource(store);
		final AkibaInformationSchema aisCopy2 = new Reader(source2).load();

		
//		new Writer(new SqlTextTarget(new PrintWriter(new FileWriter("/tmp/ais1.txt")))).save(ais);
//		new Writer(new SqlTextTarget(new PrintWriter(new FileWriter("/tmp/ais2.txt")))).save(aisCopy2);
		
		assertTrue(equals(ais, aisCopy2));

		final Source source3 = new CServerAisSource(store);
		final AkibaInformationSchema aisCopy3 = new Reader(source3).load();
		assertTrue(equals(ais, aisCopy3));
	}
	
	private boolean equals(final AkibaInformationSchema ais1, final AkibaInformationSchema ais2) throws Exception {
		final ByteBuffer bb1 = ByteBuffer.allocate(100000);
		final ByteBuffer bb2 = ByteBuffer.allocate(100000);
		
		new Writer(new MessageTarget(bb1)).save((AkibaInformationSchema)ais1);
		new Writer(new MessageTarget(bb2)).save((AkibaInformationSchema)ais2);
		bb1.flip();
		bb2.flip();
		if (bb1.limit() != bb2.limit()) {
			return false;
		}
		for (int i = 0; i < bb1.limit(); i++) {
			if (bb1.get() != bb2.get()) {
				return false;
			}
		}
		return true;
	}


}
