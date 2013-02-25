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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.akiban.direct.DirectContext;
import com.akiban.direct.DirectModule;

public class JSModule implements DirectModule {

    private File jsFile;
    
    private boolean isIdempotent;
    
    private DirectContext context;

    private CompiledScript compiled;
    
    private Bindings bindings;
    
    @Override
    public void setContext(DirectContext context) {
        this.context = context;
    }
    
    public DirectContext getContext() {
        return context;
    }
    
    @Override
    public void start() throws Exception {
        if (!jsFile.exists() || jsFile.isDirectory()) {
            throw new FileNotFoundException(jsFile.toString());
        }
        ScriptEngineManager manager = new ScriptEngineManager(getClass().getClassLoader());
        ScriptEngine engine = manager.getEngineByName("js");
        Compilable compiler = (Compilable)engine;
        compiled = compiler.compile(new FileReader(jsFile));
        bindings = engine.createBindings();
        bindings.put("dcontext", context);
    }

    @Override
    public void stop() throws Exception {
        // Nothing to do
    }

    @Override
    public boolean isIdempotent() {
        return isIdempotent;
    }

    @Override
    public Object exec(Map<String, List<String>> params) throws Exception {
        bindings.put("params", params);
        return compiled.eval(bindings);
    }

    public java.sql.Connection getConnection() {
        return context.getConnection();
    }
    
    public java.sql.Statement createStatement() throws SQLException {
        return context.getConnection().createStatement();
    }
    
    public void setParams(Map<String, List<String>> params) {
        jsFile = new File(getFirst(params, "file"));
        String isi = getFirst(params, "idempotent");
        isIdempotent = isi == null ? false : isi.equalsIgnoreCase("true");
    }

    private String getFirst(Map<String, List<String>> params, String name) {
        List<String> values = params.get(name);
        if (values != null && !values.isEmpty()) {
            return values.get(0);
        }
        return null;
    }
   
}
