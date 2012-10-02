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
package com.akiban.ais.model;

import com.akiban.ais.model.validation.AISInvariants;

import java.net.URL;
import java.util.*;

public class SQLJJar
{
    public static SQLJJar create(AkibanInformationSchema ais, 
                                 String schemaName, String name,
                                 URL url) {
        SQLJJar sqljJar = new SQLJJar(ais, schemaName, name, url);
        ais.addSQLJJar(sqljJar);
        return sqljJar; 
    }
    
    protected SQLJJar(AkibanInformationSchema ais, 
                      String schemaName, String name,
                      URL url) {
        ais.checkMutability();
        AISInvariants.checkNullName(schemaName, "SQJ/J jar", "schema name");
        AISInvariants.checkNullName(name, "SQJ/J jar", "jar name");
        AISInvariants.checkDuplicateSQLJJar(ais, schemaName, name);
        AISInvariants.checkNullField(url, "SQLJJar", "url", "URL");
        
        this.ais = ais;
        this.name = new TableName(schemaName, name);
        this.url = url;
    }
    
    public TableName getName() {
        return name;
    }

    public URL getURL() {
        return url;
    }

    public Collection<Routine> getRoutines() {
        return routines;
    }

    protected void checkMutability() {
        ais.checkMutability();
    }

    protected void addRoutine(Routine routine)
    {
        checkMutability();
        routines.add(routine);
    }

    protected void setURL(URL url) {
        checkMutability();
        this.url = url;
    }


    // State
    protected final AkibanInformationSchema ais;
    protected final TableName name;
    protected URL url;
    protected final Collection<Routine> routines = new ArrayList<Routine>();
}
