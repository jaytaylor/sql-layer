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

package com.akiban.sql.optimizer;

import com.akiban.sql.StandardException;
import com.akiban.sql.compiler.TypeComputer;
import com.akiban.sql.parser.CreateViewNode;
import com.akiban.sql.parser.FromSubquery;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SQLParserFeature;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.View;

import com.akiban.server.error.ViewHasBadSubqueryException;

import com.akiban.server.service.functions.FunctionsRegistryImpl;

import java.util.*;

/** An Akiban schema, parser and binder with various client properties.
 * Also caches view definitions.
 */
public class AISBinderContext
{
    protected Properties properties;
    protected AkibanInformationSchema ais;
    protected long aisTimestamp = -1;
    protected SQLParser parser;
    protected String defaultSchemaName;
    protected AISBinder binder;
    protected TypeComputer typeComputer;
    protected Map<View,ViewDefinition> viewDefinitions;

    /** When context is part of a larger object, such as a server session. */
    protected AISBinderContext() {
    }

    /** Standalone context used for loading views in tests and bootstrapping. */
    public AISBinderContext(AkibanInformationSchema ais, String defaultSchemaName) {
        this.ais = ais;
        this.defaultSchemaName = defaultSchemaName;
        properties = new Properties();
        properties.put("database", defaultSchemaName);
        setBinderAndTypeComputer(new AISBinder(ais, defaultSchemaName),
                                 new FunctionsTypeComputer(new FunctionsRegistryImpl()));
    }

    public Properties getProperties() {
        return properties;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defval) {
        return properties.getProperty(key, defval);
    }

    public void setProperty(String key, String value) {
        if (value == null)
            properties.remove(key);
        else
            properties.setProperty(key, value);
    }

    protected void setProperties(Properties properties) {
        this.properties = properties;
    }

    public AkibanInformationSchema getAIS() {
        return ais;
    }

    public SQLParser getParser() {
        return parser;
    }
    
    protected Set<SQLParserFeature> initParser() {
        parser = new SQLParser();
        Set<SQLParserFeature> parserFeatures = new HashSet<SQLParserFeature>();
        // TODO: Others that are on by defaults; could have override to turn them
        // off, but they are pretty harmless.
        if (Boolean.parseBoolean(getProperty("parserInfixBit", "false")))
            parserFeatures.add(SQLParserFeature.INFIX_BIT_OPERATORS);
        if (Boolean.parseBoolean(getProperty("parserInfixLogical", "false")))
            parserFeatures.add(SQLParserFeature.INFIX_LOGICAL_OPERATORS);
        if (Boolean.parseBoolean(getProperty("columnAsFunc", "false")))
            parserFeatures.add(SQLParserFeature.MYSQL_COLUMN_AS_FUNCS);
        if ("string".equals(getProperty("parserDoubleQuoted", "identifier")))
            parserFeatures.add(SQLParserFeature.DOUBLE_QUOTED_STRING);
        parser.getFeatures().addAll(parserFeatures);

        defaultSchemaName = getProperty("database");
        // TODO: Any way / need to ask AIS if schema exists and report error?

        return parserFeatures;
    }

    /** Get the non-default properties that were used to parse a view
     * definition, for example. 
     * @see #initParser
     */
    public Properties getParserProperties(String schemaName) {
        Properties properties = new Properties();
        if (!defaultSchemaName.equals(schemaName))
            properties.put("database", defaultSchemaName);
        String prop = getProperty("parserInfixBit", "false");
        if (!"false".equals(prop))
            properties.put("parserInfixBit", prop);
        prop = getProperty("parserInfixLogical", "false");
        if (!"false".equals(prop))
            properties.put("parserInfixLogical", prop);
        prop = getProperty("columnAsFunc", "false");
        if (!"false".equals(prop))
            properties.put("columnAsFunc", prop);
        prop = getProperty("parserDoubleQuoted", "identifier");
        if (!"identifier".equals(prop))
            properties.put("parserDoubleQuoted", prop);
        return properties;
    }

    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    public void setDefaultSchemaName(String defaultSchemaName) {
        this.defaultSchemaName = defaultSchemaName;
        if (binder != null)
            binder.setDefaultSchemaName(defaultSchemaName);
    }
    
    public AISBinder getBinder() {
        return binder;
    }

    public void setBinderAndTypeComputer(AISBinder binder, TypeComputer typeComputer) {
        this.binder = binder;
        binder.setContext(this);
        this.typeComputer = typeComputer;
        this.viewDefinitions = new HashMap<View,ViewDefinition>();
    }

    protected void initBinder() {
        assert (binder == null);
        setBinderAndTypeComputer(new AISBinder(ais, defaultSchemaName), null);
    }

    /** Get view definition given the AIS view. */
    public ViewDefinition getViewDefinition(View view) {
        ViewDefinition viewdef = viewDefinitions.get(view);
        if (viewdef == null) {
            viewdef = new ViewReloader(view, this).getViewDefinition(view.getDefinition());
            viewDefinitions.put(view, viewdef);
        }
        return viewdef;
    }

    /** When reloading a view from AIS, we need to parse the
     * definition text again in the same parser environment as it was
     * originally defined. Also the present binder is in the middle of
     * something so we need a separate one.
     */
    protected static class ViewReloader extends AISBinderContext {
        public ViewReloader(View view, AISBinderContext parent) {
            ais = view.getAIS();
            properties = view.getDefinitionProperties();
            initParser();
            setBinderAndTypeComputer(new AISBinder(ais, defaultSchemaName),
                                     parent.typeComputer);
        }
    }

    /** Get view definition given the user's DDL. */
    public ViewDefinition getViewDefinition(CreateViewNode ddl) {
        try {
            ViewDefinition view = new ViewDefinition(ddl, parser);
            binder.bind(view.getSubquery(), false);
            if (typeComputer != null)
                view.getSubquery().accept(typeComputer);
            return view;
        }
        catch (StandardException ex) {
            throw new ViewHasBadSubqueryException(ddl.getObjectName().toString(), 
                                                  ex.getMessage());
        }
    }

    /** Get view definition using stored copy of original DDL. */
    public ViewDefinition getViewDefinition(String ddl) {
        ViewDefinition view = null;
        try {
            view = new ViewDefinition(ddl, parser);
            binder.bind(view.getSubquery(), true);
            if (typeComputer != null)
                view.getSubquery().accept(typeComputer);
        }
        catch (StandardException ex) {
            String name = ddl;
            if (view != null)
                name = view.getName().toString();
            throw new ViewHasBadSubqueryException(name, ex.getMessage());
        }
        return view;
    }

    public Collection<Columnar> getTableReferences(ViewDefinition view) {
        return Collections.emptySet();
    }

    // TODO: Only used by tests. Remove when they go through ViewDDL.
    public void addView(ViewDefinition view) {
        throw new UnsupportedOperationException();
    }

}
