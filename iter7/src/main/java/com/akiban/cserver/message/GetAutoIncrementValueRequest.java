package com.akiban.cserver.message;

import java.nio.ByteBuffer;

import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.CServer.CServerContext;
import com.akiban.message.AkibaConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.message.Request;

public class GetAutoIncrementValueRequest extends Request implements CServerConstants {
	
	public static short TYPE;
	
	private int rowDefId;

	public int getRowDefId() {
		return rowDefId;
	}


	public void setRowDefId(int rowDefId) {
		this.rowDefId = rowDefId;
	}


	public GetAutoIncrementValueRequest() {
		super(TYPE);
	}
	
	public GetAutoIncrementValueRequest(final int rowDefId) {
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
		final long value = ((CServerContext)context).getStore().getAutoIncrementValue(rowDefId);
		connection.send(new GetAutoIncrementValueResponse(rowDefId, OK, value));
	}

}
