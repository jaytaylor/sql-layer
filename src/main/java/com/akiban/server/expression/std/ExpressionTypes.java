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
    public static final ExpressionType DATE = new ExpressionTypeImpl(AkType.DATE);
    public static final ExpressionType DATETIME = new ExpressionTypeImpl(AkType.DATETIME);
    public static final ExpressionType DOUBLE = new ExpressionTypeImpl(AkType.DOUBLE);
    public static final ExpressionType FLOAT = new ExpressionTypeImpl(AkType.FLOAT);
    public static final ExpressionType INT = new ExpressionTypeImpl(AkType.INT);
    public static final ExpressionType LONG = new ExpressionTypeImpl(AkType.LONG);
    public static final ExpressionType TIME = new ExpressionTypeImpl(AkType.TIME);
    public static final ExpressionType TIMESTAMP = new ExpressionTypeImpl(AkType.TIMESTAMP);
    public static final ExpressionType U_BIGINT = new ExpressionTypeImpl(AkType.U_BIGINT);
    public static final ExpressionType U_DOUBLE = new ExpressionTypeImpl(AkType.U_DOUBLE);
    public static final ExpressionType U_FLOAT = new ExpressionTypeImpl(AkType.U_FLOAT);
    public static final ExpressionType U_INT = new ExpressionTypeImpl(AkType.U_INT);
    public static final ExpressionType YEAR = new ExpressionTypeImpl(AkType.YEAR);
    public static final ExpressionType BOOL = new ExpressionTypeImpl(AkType.BOOL);

    public static ExpressionType decimal(int precision, int scale) {
        return new ExpressionTypeImpl(AkType.DECIMAL, precision, scale);
    }

    public static ExpressionType varchar(int length) {
        return new ExpressionTypeImpl(AkType.VARCHAR, length);
    }

    public static ExpressionType varbinary(int length) {
        return new ExpressionTypeImpl(AkType.VARBINARY, length);
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
        
        ExpressionTypeImpl(AkType type) {
            this(type, 0, 0);
        }

        ExpressionTypeImpl(AkType type, int precision) {
            this(type, precision, 0);
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