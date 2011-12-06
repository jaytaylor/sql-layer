/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */


package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionType;

import com.akiban.server.types.AkType;

public class ExpressionTypes
{
    public static final ExpressionType BOOL = newType(AkType.BOOL);
    public static final ExpressionType DATE = newType(AkType.DATE);
    public static final ExpressionType DATETIME = newType(AkType.DATETIME);
    public static final ExpressionType DOUBLE = newType(AkType.DOUBLE);
    public static final ExpressionType FLOAT = newType(AkType.FLOAT);
    public static final ExpressionType INT = newType(AkType.INT);
    public static final ExpressionType LONG = newType(AkType.LONG);
    public static final ExpressionType NULL = newType(AkType.NULL);
    public static final ExpressionType TEXT = newType(AkType.TEXT);
    public static final ExpressionType TIME = newType(AkType.TIME);
    public static final ExpressionType TIMESTAMP = newType(AkType.TIMESTAMP);
    public static final ExpressionType U_BIGINT = newType(AkType.U_BIGINT);
    public static final ExpressionType U_DOUBLE = newType(AkType.U_DOUBLE);
    public static final ExpressionType U_FLOAT = newType(AkType.U_FLOAT);
    public static final ExpressionType U_INT = newType(AkType.U_INT);
    public static final ExpressionType YEAR = newType(AkType.YEAR);
    public static final ExpressionType INTERVAL = newType(AkType.INTERVAL);

    public static ExpressionType decimal(int precision, int scale) {
        return newType(AkType.DECIMAL, precision, scale);
    }

    public static ExpressionType varchar(int length) {
        return newType(AkType.VARCHAR, length);
    }

    public static ExpressionType varbinary(int length) {
        return newType(AkType.VARBINARY, length);
    }

    private static ExpressionType newType(AkType type) {
        return new ExpressionTypeImpl(type, 0, 0);
    }

    private static ExpressionType newType(AkType type, int precision) {
        return new ExpressionTypeImpl(type, precision, 0);
    }

    public static ExpressionType newType(AkType type, int precision, int scale) {
        return new ExpressionTypeImpl(type, precision, scale);
    }

    static class ExpressionTypeImpl implements ExpressionType {
        @Override
        public AkType getType() {
            return type;
        }

        @Override
        public int getPrecision() {
            return precision;
        }
        
        @Override
        public int getScale() {
            return scale;
        }

        @Override
        public String toString() {
            return type + "(" + precision + "," + scale + ")";
        }
        
        ExpressionTypeImpl(AkType type, int precision, int scale) {
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }

        private AkType type;
        private int precision, scale;
    }
    
    private ExpressionTypes() {
    }
}