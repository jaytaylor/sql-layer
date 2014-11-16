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
package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;

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

    public void removeRoutine(Routine routine) {
        routines.remove(routine);
    }

    public void setURL(URL url) {
        checkMutability();
        this.url = url;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    // State
    protected final AkibanInformationSchema ais;
    protected final TableName name;
    protected URL url;
    protected long version;
    protected transient final Collection<Routine> routines = new ArrayList<>();
}
