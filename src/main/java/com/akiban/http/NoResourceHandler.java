/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
