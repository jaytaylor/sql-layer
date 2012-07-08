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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SQLParserFeature;

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

    public void setBinder(AISBinder binder) {
        this.binder = binder;
        binder.setContext(this);
    }

    protected void initBinder() {
        assert (binder == null);
        setBinder(new AISBinder(ais, defaultSchemaName));
    }

}
