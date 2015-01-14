/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.CreateViewNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserFeature;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.error.ViewHasBadSubqueryException;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;

import java.util.*;

/** A schema, parser and binder with various client properties.
 * Also caches view definitions.
 */
public class AISBinderContext
{
    public static final String CONFIG_PARSER_FEATURES = "parserFeatures";

    protected Properties properties;
    protected AkibanInformationSchema ais;
    protected SQLParser parser;
    protected String defaultSchemaName;
    protected AISBinder binder;
    protected TypeComputer typeComputer;
    protected Map<View,AISViewDefinition> viewDefinitions;

    /** When context is part of a larger object, such as a server session. */
    protected AISBinderContext() {
    }

    /** Standalone context used for loading views in tests and bootstrapping. */
    public AISBinderContext(AkibanInformationSchema ais, String defaultSchemaName) {
        this.ais = ais;
        this.defaultSchemaName = defaultSchemaName;
        properties = new Properties();
        properties.put("database", defaultSchemaName);
        initParser();        
        setBinderAndTypeComputer(new AISBinder(ais, defaultSchemaName),
                                 new FunctionsTypeComputer(TypesRegistryServiceImpl.createRegistryService()));
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
        Set<SQLParserFeature> parserFeatures = getParserFeatures();
        parser = new SQLParser();
        parser.getFeatures().clear();
        parser.getFeatures().addAll(parserFeatures);

        if (defaultSchemaName == null) {
            defaultSchemaName = getProperty("database");
            if (defaultSchemaName == null)
                defaultSchemaName = getProperty("user");
        }
        // TODO: Any way / need to ask AIS if schema exists and report error?

        BindingNodeFactory.wrap(parser);

        return parserFeatures;
    }

    protected Set<SQLParserFeature> getParserFeatures() {
        Set<SQLParserFeature> features = new HashSet<>();
        String featuresStr = getProperty(CONFIG_PARSER_FEATURES);
        if (featuresStr != null) {
            String[] featureNames = featuresStr.split(",");
            for(String f : featureNames) {
                try {
                    features.add(SQLParserFeature.valueOf(f));
                } catch(IllegalArgumentException e) {
                    throw new InvalidParameterValueException("'" + f + "' for " + CONFIG_PARSER_FEATURES);
                }
            }
        }
        if (getBooleanProperty("parserInfixBit", false)) {
            features.add(SQLParserFeature.INFIX_BIT_OPERATORS);
        }
        if (getBooleanProperty("parserInfixLogical", false)) {
            features.add(SQLParserFeature.INFIX_LOGICAL_OPERATORS);
        }
        if (getBooleanProperty("columnAsFunc", false)) {
            features.add(SQLParserFeature.MYSQL_COLUMN_AS_FUNCS);
        }
        String prop = getProperty("parserDoubleQuoted", "identifier");
        if (prop.equals("string")) {
            features.add(SQLParserFeature.DOUBLE_QUOTED_STRING);
        } else if (!prop.equals("identifier")) {
            throw new InvalidParameterValueException("'" + prop + "' for parserDoubleQuoted");
        }
        return features;
    }

    public boolean getBooleanProperty(String key, boolean defval) {
        String prop = getProperty(key);
        if (prop == null) return defval;
        if (prop.equalsIgnoreCase("true"))
            return true;
        else if (prop.equalsIgnoreCase("false"))
            return false;
        else
            throw new InvalidParameterValueException("'" + prop + "' for " + key);
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
        this.viewDefinitions = new HashMap<>();
    }

    protected void initBinder() {
        assert (binder == null);
        setBinderAndTypeComputer(new AISBinder(ais, defaultSchemaName), null);
    }

    /** Get view definition given the AIS view. */
    public AISViewDefinition getViewDefinition(View view) {
        AISViewDefinition viewdef = viewDefinitions.get(view);
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
            if (defaultSchemaName == null)
                defaultSchemaName = view.getName().getSchemaName();
            setBinderAndTypeComputer(new AISBinder(ais, defaultSchemaName),
                                     parent.typeComputer);
        }
    }

    /** Get view definition given the user's DDL. */
    public AISViewDefinition getViewDefinition(CreateViewNode ddl, ServerSession server) {
        try {
            // Just want the definition for result columns and table references.
            // If the view uses another view, the inner one is treated
            // like a table for those purposes.
            AISViewDefinition view = new AISViewDefinition(ddl, parser);
            binder.bind(view.getSubquery(), false);
            view.getTableColumnReferences(); // get the references before expanding views
            if (typeComputer != null) {
                typeComputer.compute(view.getSubquery());
            }
            if (!viewHasTypes(view)) {
                ViewCompiler compiler = new ViewCompiler(server, server.getServiceManager().getStore());
                compiler.findAndSetTypes(view);
            }
            return view;
        }
        catch (StandardException ex) {
            throw new ViewHasBadSubqueryException(ddl.getObjectName().toString(), 
                                                  ex.getMessage());
        }
    }

    public boolean viewHasTypes(AISViewDefinition view) {
        for (ResultColumn col : view.getResultColumns()) {
            if (col.getType() == null) return false;
        }
        return true;
    }

    /** Get view definition using stored copy of original DDL. */
    public AISViewDefinition getViewDefinition(String ddl) {
        AISViewDefinition view = null;
        try {
            view = new AISViewDefinition(ddl, parser);
            // Want a subquery that can be spliced in.
            // If the view uses another view, it gets expanded, too.
            binder.bind(view.getSubquery(), true);
            if (typeComputer != null)
                typeComputer.compute(view.getSubquery());
        }
        catch (StandardException ex) {
            String name = ddl;
            if (view != null)
                name = view.getName().toString();
            throw new ViewHasBadSubqueryException(name, ex.getMessage());
        }
        return view;
    }

    public boolean isAccessible(TableName object) {
        return isSchemaAccessible(object.getSchemaName());
    }

    public boolean isSchemaAccessible(String schemaName) {
        return true;
    }

}
