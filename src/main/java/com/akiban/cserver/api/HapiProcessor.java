package com.akiban.cserver.api;
import java.nio.ByteBuffer;

public interface HapiProcessor {
	public String processRequest(String request, ByteBuffer byteBuffer) ;	
	public String processRequest(String request ) ;
}
