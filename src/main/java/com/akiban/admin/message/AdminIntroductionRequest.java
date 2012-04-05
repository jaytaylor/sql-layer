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

package com.akiban.admin.message;

import java.io.File;
import java.io.IOException;

import com.akiban.message.MessageRequiredServices;
import com.akiban.server.service.session.Session;
import com.akiban.message.AkibanSendConnection;
import com.akiban.message.Request;
import com.akiban.util.GrowableByteBuffer;

public class AdminIntroductionRequest extends Request
{
    // Request interface

    @Override
    public void read(GrowableByteBuffer payload) throws Exception
    {
        super.read(payload);
        adminInitializer = readString(payload);
    }

    @Override
    public void write(GrowableByteBuffer payload) throws Exception
    {
        super.write(payload);
        writeString(payload, adminInitializer);
    }

    @Override
    public void execute(AkibanSendConnection connection, Session session, MessageRequiredServices requiredServices) throws Exception
    {
        // Executes in mysql head
    }

    // AdminIntroductionRequest interface

    public AdminIntroductionRequest(String adminInitializer) throws IOException
    {
        super(TYPE);
        // Have to send an absolute path to the other side
        this.adminInitializer = new File(adminInitializer).getCanonicalPath();
    }

    public AdminIntroductionRequest()
    {
        super(TYPE);
    }

    // State

    public static short TYPE;

    private String adminInitializer;
}
