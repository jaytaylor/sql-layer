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

package com.akiban.sql.server;

import com.akiban.server.types3.pvalue.PBasicValueTarget;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.AkibanAppender;

import java.io.UnsupportedEncodingException;

public final class AppenderPValueTarget implements PValueTarget {

    public void setUnderlying(PUnderlying underlying) {
        this.underlying = underlying;
    }

    @Override
    public boolean supportsCachedObjects() {
        return false;
    }

    @Override
    public void putValueSource(PValueSource source) {
        PValueTargets.copyFrom(source, this);
    }

    @Override
    public void putObject(Object object) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PUnderlying getUnderlyingType() {
        return underlying;
    }

    @Override
    public void putNull() {
        appender.append("null");
    }

    @Override
    public void putBool(boolean value) {
        appender.append(Boolean.toString(value));
    }

    @Override
    public void putInt8(byte value) {
        appender.append(value);
    }

    @Override
    public void putInt16(short value) {
        appender.append(value);
    }

    @Override
    public void putUInt16(char value) {
        appender.append(value);
    }

    @Override
    public void putInt32(int value) {
        appender.append(value);
    }

    @Override
    public void putInt64(long value) {
        appender.append(value);
    }

    @Override
    public void putFloat(float value) {
        appender.append(Float.toString(value));
    }

    @Override
    public void putDouble(double value) {
        appender.append(Double.toString(value));
    }

    @Override
    public void putBytes(byte[] value) {
        String s;
        try {
            s = new String(value, "latin1");
        } catch (UnsupportedEncodingException e) {
            throw new UnsupportedOperationException(e);
        }
        appender.append(s);
    }

    @Override
    public void putString(String value) {
        appender.append(value);
    }

    public AppenderPValueTarget(AkibanAppender appender) {
        this.appender = appender;
    }

    private AkibanAppender appender;
    private PUnderlying underlying;
}
