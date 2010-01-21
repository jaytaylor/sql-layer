package com.akiba.cserver.message;

import com.akiba.message.Message;

public class ScanMoreRequest extends Message {

	public static short TYPE;

	public ScanMoreRequest() {
		super(TYPE);
	}

}
