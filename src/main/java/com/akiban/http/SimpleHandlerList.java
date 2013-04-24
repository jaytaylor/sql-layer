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

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.MultiException;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * <p>
 *     A {@link Handler} that delegates to one or more other handlers.
 * </p>
 * <p>
 *     In the style of {@link org.eclipse.jetty.server.handler.HandlerCollection} but is thread safe,
 *     doesn't adjust the running state of other handlers upon add or remove, and stops processing
 *     a request once it has been handled.
 * </p>
 */
public class SimpleHandlerList extends AbstractHandler {
    private final List<Handler> handlers = new CopyOnWriteArrayList<>();
    private Handler defaultHandler = null;
    public static final InOutTap REST_TAP = Tap.createTimer("rest: root");


    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
        if(!isStarted()) {
            return;
        }
        REST_TAP.in();
        try {
            for(Handler h : handlers) {
                h.handle(target,baseRequest, request, response);
                // Return once the request has been handled
                if(baseRequest.isHandled()) {
                    return;
                }
            }
            if (defaultHandler != null) {
                defaultHandler.handle(target, baseRequest, request, response);
            }
        }finally {
            REST_TAP.out();
        }
    }

    @Override
    protected void doStart() throws Exception {
        MultiException mex = new MultiException();
        try {
            super.doStart();
        } catch(Exception e) {
            mex.add(e);
        }
        for(Handler h : handlers) {
            try {
                h.start();
            } catch(Exception e){
                mex.add(e);
            }
        }
        try {
            if (defaultHandler != null) {
                defaultHandler.start();
            }
        } catch (Exception e) {
            mex.add(e);
        }
        mex.ifExceptionThrow();
    }

    @Override
    protected void doStop() throws Exception {
        MultiException mex = new MultiException();
        for(Handler h : handlers) {
            try{
                h.stop();
            } catch(Exception e) {
                mex.add(e);
            }
        }
        try {
            if (defaultHandler != null) {
                defaultHandler.stop();
            }
        } catch (Exception e) {
            mex.add(e);
        }
        try {
            super.doStop();
        } catch(Exception e){
            mex.add(e);
        }
        mex.ifExceptionThrow();
    }

    @Override
    public void destroy() {
        MultiException mex = new MultiException();
        for(Handler h : handlers) {
            try {
                h.destroy();
            } catch(Exception e) {
                mex.add(e);
            }
        }
        try {
            if (defaultHandler != null) {
                defaultHandler.destroy();
            }
        } catch (Exception e) {
            mex.add(e);
        }

        try {
            super.destroy();
        } catch(Exception e) {
            mex.add(e);
        }
        mex.ifExceptionThrowRuntime();
    }

    public void addHandler(Handler h) {
        if(h.getServer() != getServer()) {
            h.setServer(getServer());
        }
        handlers.add(h);
    }

    public void removeHandler(Handler h) {
        handlers.remove(h);
    }
    
    public void addDefaultHandler(Handler h) {
        if (h.getServer() != getServer()) {
            h.setServer(getServer());
        }
        defaultHandler = h;
    }
}
