package com.akiba.cserver.message;

import java.nio.ByteBuffer;

import com.akiba.cserver.CServerConstants;
import com.akiba.cserver.CServer.CServerContext;
import com.akiba.message.AkibaConnection;
import com.akiba.message.ExecutionContext;
import com.akiba.message.Message;

public class DropTableRequest extends Message implements CServerConstants {
	
	public static short TYPE;
	
	private int rowDefId;

	public int getRowDefId() {
		return rowDefId;
	}


	public void setRowDefId(int rowDefId) {
		this.rowDefId = rowDefId;
	}


	public DropTableRequest() {
		super(TYPE);
	}
	
	public DropTableRequest(final int rowDefId) {
		super(TYPE);
		this.rowDefId = rowDefId;
	}
	

	@Override
	public void read(ByteBuffer payload) throws Exception
    {
		super.read(payload);
		rowDefId = payload.getInt();
	}

	@Override
	public void write(ByteBuffer payload) throws Exception
    {
		super.write(payload);
		payload.putInt(rowDefId);
	}
	
	@Override
	public void execute(final AkibaConnection connection, final ExecutionContext context) throws Exception {
		final int result = ((CServerContext)context).getStore().dropTable(rowDefId);
		connection.send(new DropTableResponse(result));
	}

}
