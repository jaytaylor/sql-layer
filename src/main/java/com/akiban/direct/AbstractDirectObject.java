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

package com.akiban.direct;

import java.math.BigDecimal;
import java.util.Date;

import com.akiban.qp.row.Row;

public class AbstractDirectObject implements DirectObject {

    private Row row;
    
    @Override
    public void row(Row row) {
        this.row = row;
    }

    @Override
    public Row row() {
        return row;
    }
    

    public void save() {
        // TODO
    }
    
    protected boolean __getBOOL(int p) {
        return row.pvalue(p).getBoolean();
    }
    
    protected Date __getDATE(int p) {
        return new Date(row.pvalue(p).getInt64());
    }
    
    protected Date __getDATETIME(int p) {
        return new Date(row.pvalue(p).getInt64());
    }
    
    protected BigDecimal __getDECIMAL(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a DECIMAL from a ValueSource");
    }

    protected double __getDOUBLE(int p) {
        return row.pvalue(p).getDouble();
    }
    
    protected float __getFLOAT(int p) {
        return row.pvalue(p).getFloat();
    }
    
    protected int __getINT(int p) {
        return row.pvalue(p).getInt32();
    }
    
    protected int __getINTERVAL_MILLIS(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a INTERVAL_MILLIS from a ValueSource");
    }

    protected int __getINTERVAL_MONTH(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a INTERVAL_MONTH from a ValueSource");
    }
    
    protected long __getLONG(int p) {
        return row.pvalue(p).getInt64();
    }
    
    protected Object __getNULL(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a NULL from a ValueSource");
    }
    
    protected Object __getRESULT_SET(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a RESULT_SET from a ValueSource");
    }
    
    protected String __getTEXT(int p) {
        return row.pvalue(p).getString();
    }

    protected long __getTIME(int p) {
        return row.pvalue(p).getInt64();
    }
    
    protected long __getTIMESTAMP(int p) {
        return row.pvalue(p).getInt64();
    }
    
    protected long __getU_INT(int p) {
        return row.pvalue(p).getInt64();
    }

    protected Object __getU_BIGINT(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a U_BIGINT from a ValueSource");
    }
    
    protected Object __getU_DOUBLE(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a U)DOUBLE from a ValueSource");
    }
    
    protected double __getU_FLOAT(int p) {
        return row.pvalue(p).getDouble();
    }

    protected String __getVARCHAR(int p) {
        return row.pvalue(p).getString();
    }
    
    protected byte[] __getVARBINARY(int p) {
        return row.pvalue(p).getBytes();
    }

    protected int __getYEAR(int p) {
        return row.pvalue(p).getInt32();
    }
    
}
