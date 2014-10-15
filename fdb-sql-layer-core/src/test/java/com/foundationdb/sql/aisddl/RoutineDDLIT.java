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

package com.foundationdb.sql.aisddl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.NoSuchRoutineException;

import com.ibm.icu.text.MessageFormat;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;

public class RoutineDDLIT extends AISDDLITBase {
    @Before
    public void createJar() throws Exception {
        // CALL SQLJ.INSTALL_JAR('foo.jar', 'ajar', 0)
        AISBuilder builder = new AISBuilder();
        builder.sqljJar(SCHEMA_NAME, "ajar", new URL("file://foo.jar"));
        ddl().createSQLJJar(session(), builder.akibanInformationSchema().getSQLJJar(SCHEMA_NAME, "ajar"));
        updateAISGeneration();
    }

    @Test
    public void testCreateJava() throws Exception {
        executeDDL("CREATE PROCEDURE proca(IN x INT, OUT d DOUBLE) LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'ajar:com.acme.Procs.aproc'");
        Routine proc = ais().getRoutine(SCHEMA_NAME, "proca");
        assertNotNull(proc);
        assertEquals("java", proc.getLanguage());
        assertEquals(Routine.CallingConvention.JAVA, proc.getCallingConvention());
        assertEquals(2, proc.getParameters().size());
        assertEquals("x", proc.getParameters().get(0).getName());
        assertEquals(Parameter.Direction.IN, proc.getParameters().get(0).getDirection());
        assertEquals("INT", proc.getParameters().get(0).getTypeName());
        assertEquals("d", proc.getParameters().get(1).getName());
        assertEquals("DOUBLE", proc.getParameters().get(1).getTypeName());
        assertEquals(Parameter.Direction.OUT, proc.getParameters().get(1).getDirection());
        assertEquals(Routine.SQLAllowed.READS_SQL_DATA, proc.getSQLAllowed());
        assertEquals(1, proc.getDynamicResultSets());
    }

    @Test(expected=NoSuchRoutineException.class)
    public void testDropNonexistent() throws Exception {
        executeDDL("DROP PROCEDURE no_such_proc");
    }

    @Test
    public void testDropExists() throws Exception {
        executeDDL("CREATE PROCEDURE procb() LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'ajar:com.acme.Procs.bproc'");
        assertNotNull(ais().getRoutine(SCHEMA_NAME, "procb"));

        executeDDL("DROP PROCEDURE procb");
        assertNull(ais().getRoutine(SCHEMA_NAME, "procb"));
    }
    
    @Test 
    public void testDropIfExists() throws Exception {
        String sql = "DROP PROCEDURE IF EXISTS no_such_proc";
        executeDDL(sql);
        assertEquals(Collections.singletonList(MessageFormat.format(ErrorCode.NO_SUCH_ROUTINE.getMessage(), SCHEMA_NAME, "no_such_proc")), getWarnings());
    }

}
