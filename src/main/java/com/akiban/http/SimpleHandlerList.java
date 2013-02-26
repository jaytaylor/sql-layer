/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.http;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.MultiException;

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

    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
        if(!isStarted()) {
            return;
        }
        for(Handler h : handlers) {
            h.handle(target,baseRequest, request, response);
            // Return once the request has been handled
            if(baseRequest.isHandled()) {
                return;
            }
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
}
