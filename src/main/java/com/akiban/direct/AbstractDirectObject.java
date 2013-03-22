/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.direct;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import com.akiban.sql.server.ServerJavaValues;

public class AbstractDirectObject implements DirectObject {

    private ServerJavaValues values;
    private DirectResultSet rs;

    public void setResults(ServerJavaValues values, DirectResultSet rs) {
        this.values = values;
        this.rs = rs;
    }
    
    public ServerJavaValues values() {
        if (rs.hasRow()) {
            return values;
        }
        throw new IllegalStateException("No more rows");
    }

    public void save() {
        // TODO
    }

    protected boolean __getBOOL(int p) {
        return values().getBoolean(p);
    }

    protected Date __getDATE(int p) {
        return values().getDate(p);
    }

    protected Date __getDATETIME(int p) {
        return values().getDate(p);
    }

    protected BigDecimal __getDECIMAL(int p) {
        return values().getBigDecimal(p);
    }

    protected double __getDOUBLE(int p) {
        return values().getDouble(p);
    }

    protected float __getFLOAT(int p) {
        return values().getFloat(p);
    }

    protected int __getINT(int p) {
        return values().getInt(p);
    }

    protected int __getINTERVAL_MILLIS(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a INTERVAL_MILLIS from a ValueSource");
    }

    protected int __getINTERVAL_MONTH(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a INTERVAL_MONTH from a ValueSource");
    }

    protected long __getLONG(int p) {
        return values().getLong(p);
    }

    protected Object __getNULL(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a NULL from a ValueSource");
    }

    protected Object __getRESULT_SET(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a RESULT_SET from a ValueSource");
    }

    protected String __getTEXT(int p) {
        return values().getString(p);
    }

    protected Time __getTIME(int p) {
        return values().getTime(p);
    }

    protected Timestamp __getTIMESTAMP(int p) {
        return values().getTimestamp(p);
    }

    protected long __getU_INT(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a U_INT from a ValueSource");
    }

    protected BigInteger __getU_BIGINT(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a U_BIGINT from a ValueSource");
    }

    protected BigDecimal __getU_DOUBLE(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a U_DOUBLE from a ValueSource");
    }

    protected double __getU_FLOAT(int p) {
        throw new UnsupportedOperationException("Don't know how to convert a U_FLOAT from a ValueSource");
    }

    protected String __getVARCHAR(int p) {
        return values().getString(p);
    }

    protected byte[] __getVARBINARY(int p) {
        return values().getBytes(p);
    }

    protected int __getYEAR(int p) {
        return values().getInt(p);
    }

}
