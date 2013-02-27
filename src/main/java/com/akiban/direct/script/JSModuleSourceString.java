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
package com.akiban.direct.script;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import com.akiban.direct.DirectContext;
import com.akiban.direct.DirectModule;
import com.akiban.direct.script.JSModule.Source;
import com.akiban.rest.resources.DirectResource;

/**
 * <p>
 * Adapter to load, compile and run Javascript code as {@link DirectModule}.
 * This implementation takes a string supplied by a PUT operation as the source
 * code. This implementation takes a path specified as a parameter to read JS
 * source code. A WatchService is set up to watch for changes in this file; if
 * the file changes the new version is automatically compiled and installed at
 * the next call to {@link #eval(Map)}.
 * </p>
 * <p>
 * Example:
 * 
 * <code><pre>
 * curl -X PUT "localhost:9000/api/direct/module/test
 *           ?language=js&name=Sample&idempotent=true" -d @sample.js
 *           
 * curl -X GET localhost:9000/api/direct/Sample/test
 * </pre></code>
 * 
 * The first curl command loads a Javascript module named sample.js and installs
 * it with the endpoint name "Sample". It is declared to be idempotent which
 * means its {@link #eval(Map)} method is called in response to a GET request,
 * as exemplified by the second curl command.
 * </p>
 * <p>
 * Note that DirectResource class creates a parameter named "source" in the Map
 * sent to {@link #setParams(Map)} when given a payload byte array.
 * 
 * </p>
 * 
 * @author peter
 */
public class JSModuleSourceString implements Source {

    String source;

    /**
     * Called when the module is loaded, after
     * {@link #setContext(DirectContext)} and {@link #setParams(Map)}.
     * 
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
    }

    /**
     * Called when this module is removed.
     * 
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
    }

    /**
     * Receive parameters prior to invocation of {@link #start()}. The
     * parameters are those provided with the PUT operation that is loading this
     * module. Note that this is an optional operation for a DirectModule
     * implementation; it is discovered and called by reflection from the
     * {@link DirectResource}. In this implementation, the parameters named
     * <code>file</code> and <code>get</code> are significant.
     * <ul>
     * <li><code>file</code> is the file name of a file containing Javascript
     * source code to load</li>
     * <li><code>get</code> is "true" to indicate this module does not affect
     * server state and may be invoked through GET requests</li>
     * </ul>
     * 
     * @param params
     *            Map in the form of a {@link javax.ws.rs.core.MultivaluedMap}.
     */
    @Override
    public void setParams(Map<String, List<String>> params) {
        source = JSModule.getFirst(params, "source");
    }

    @Override
    public Reader getReader() throws Exception {
        return new StringReader(source);
    }

    @Override
    public boolean isChanged() {
        return false;
    }
}
