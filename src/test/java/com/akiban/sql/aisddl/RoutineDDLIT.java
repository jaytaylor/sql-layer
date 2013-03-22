
package com.akiban.sql.aisddl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.NoSuchRoutineException;

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
        assertEquals(Types.INT, proc.getParameters().get(0).getType());
        assertEquals("d", proc.getParameters().get(1).getName());
        assertEquals(Types.DOUBLE, proc.getParameters().get(1).getType());
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
