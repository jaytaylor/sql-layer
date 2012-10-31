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
import com.akiban.server.error.NoSuchRoutineException;

import java.net.URL;
import java.util.Collection;

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

}
