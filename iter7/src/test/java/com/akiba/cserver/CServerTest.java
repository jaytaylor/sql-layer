package com.akiba.cserver;

import java.io.File;

import junit.framework.TestCase;

import com.akiba.cserver.store.PersistitStore;
import com.akiba.message.AkibaConnection;
import com.akiba.network.AkibaNetworkHandler;
import com.akiba.network.NetworkHandlerFactory;

public class CServerTest extends TestCase {

	private final static File DATA_PATH = new File("/tmp/data");

	@Override
	public void setUp() throws Exception {
		Util.cleanUpDirectory(DATA_PATH);
		PersistitStore.setDataPath(DATA_PATH.getPath());

	}

	public void testCServer() throws Exception {
		final CServer cserver = new CServer();
		cserver.start();
		try {
	        final AkibaNetworkHandler networkHandler = NetworkHandlerFactory.getHandler("localhost", "8080", null);
	        final AkibaConnection connection = AkibaConnection.createConnection(networkHandler);
	        
	        final WriteRowRequest request = createWriteRowRequest();
	        
	        final WriteRowResponse response = (WriteRowResponse)connection.sendAndReceive(request);
	        assertEquals(100, response.getResultCode());
	        networkHandler.disconnectWorker();
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			cserver.stop();
		}
	}
	
	private WriteRowRequest createWriteRowRequest() {
        final RowDef rowDef = new RowDef(1234, new FieldDef[]{new FieldDef(FieldType.INT),new FieldDef(FieldType.INT),new FieldDef(FieldType.INT)});
        final RowData rowData = new RowData(new byte[64]);
        rowData.createRow(rowDef, new Object[]{1, 2, 3});
        final WriteRowRequest request = new WriteRowRequest();
        request.setRowData(rowData);
        return request;
	}

}
