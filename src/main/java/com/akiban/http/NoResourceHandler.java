package com.akiban.http;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.DefaultHandler;

import com.akiban.rest.RestResponseBuilder;
import com.akiban.server.error.ErrorCode;

public class NoResourceHandler extends DefaultHandler {
    
    public NoResourceHandler() {
        
    }
    
    @Override
    public void handle(String target,
            Request baseRequest,
            HttpServletRequest request,
            HttpServletResponse response)
              throws IOException,
                     ServletException {
        if (response.isCommitted() || baseRequest.isHandled())
            return;
        baseRequest.setHandled(true);

        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        response.setContentType(MediaType.APPLICATION_JSON);
        
        StringBuilder builder = new StringBuilder();
        
        RestResponseBuilder.formatJsonError(builder, ErrorCode.MALFORMED_REQUEST.getFormattedValue(), "Path not supported; use /v1/");
        builder.append('\n');
        
        response.setContentLength(builder.length());
        OutputStream out=response.getOutputStream();
        out.write(builder.toString().getBytes());
        out.close();
        
       
    }
}
