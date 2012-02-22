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

package com.akiban.ais.ddl;

import java.io.PrintWriter;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.metamodel.Target;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Types;

public class SqlTextTarget extends Target {
	// Target interface

	public SqlTextTarget(final PrintWriter writer) {
		this.writer = writer;
	}

	@Override
	public void deleteAll() {
//        prepareStatement(MetaModel.only().definition(type).cleanupQuery()).executeUpdate();
//        prepareStatement(MetaModel.only().definition(group).cleanupQuery()).executeUpdate();
//        prepareStatement(MetaModel.only().definition(table).cleanupQuery()).executeUpdate();
//        prepareStatement(MetaModel.only().definition(column).cleanupQuery()).executeUpdate();
//        prepareStatement(MetaModel.only().definition(join).cleanupQuery()).executeUpdate();
//        prepareStatement(MetaModel.only().definition(joinColumn).cleanupQuery()).executeUpdate();
//        prepareStatement(MetaModel.only().definition(index).cleanupQuery()).executeUpdate();
//        prepareStatement(MetaModel.only().definition(indexColumn).cleanupQuery()).executeUpdate();
	}

	@Override
	public void writeCount(int count) {
	}

	public void close() {
	}
	
    public void writeType(Map<String, Object> map)
    {
    	// Don't write the Types table inserts
    }



	// For use by this class
    @Override 
    public void writeVersion(int modelVersion)
    {
        final StringBuffer sb = new StringBuffer();
        sb.append("/* produced using AIS model version: ");
        sb.append(modelVersion);
        sb.append (" */");
        writer.println(sb);
    }
	@Override
	protected void write(String typename, Map<String, Object> map) {
		ModelObject modelObject = MetaModel.only().definition(typename);
		FakePreparedStatement stmt = prepareStatement(modelObject.writeQuery());
		int c = 0;
		for (ModelObject.Attribute attribute : modelObject.attributes()) {
			bind(stmt, ++c, map.get(attribute.name()));
		}
		int updateCount = stmt.executeUpdate();
		assert updateCount == 1;
	}

	private class FakePreparedStatement {
		final String statement;
		final StringBuilder sb;

		FakePreparedStatement(final String statement) {
			this.statement = statement;
			this.sb = new StringBuilder(statement);
		}

		int executeUpdate() {
			int p = sb.indexOf("(");
			int q = sb.indexOf(")");
			if (p > 0 && q > p) {
				sb.replace(p, q + 1, "");
			}
			// for ( int p; (p = sb.indexOf("    ")) != -1;) {
			// sb.replace(p, p+3, "\n ");
			// }
			sb.append(";");
			writer.println(sb);
			sb.setLength(0);
			sb.append(statement);
			return 1;
		}

		void bind(Object value) {
			final int position = sb.indexOf("?");
			String v;
			if (value == null) {
				v = "null";
			} else if (value instanceof Number) {
				v = value.toString();
			} else if (value instanceof Boolean) {
				v = ((Boolean) value).booleanValue() ? "1" : "0";
			} else {
				v = "'" + value.toString() + "'";
			}

			sb.replace(position, position + 1, v);
		}
	}

	public void writeGroupTableDDL(final AkibanInformationSchema ais) throws Exception {
		for (final GroupTable table : ais.getGroupTables().values()) {
			writer.println();
			writer.print("create table `" + table.getName().getSchemaName() + "`.`"
					+ table.getName().getTableName() + "`(");
			final int nColumns = table.getColumns().size();
			final Column[] columns = new Column[nColumns];
			for (final Column column : table.getColumns()) {
				assert columns[column.getPosition()] == null;
				columns[column.getPosition()] = column;
			}
			for (int position = 0; position < nColumns; position++) {
				final Column column = columns[position];
				writer.println(position > 0 ? "," : "");
				writer.print("  ");
				writer.print("`" + column.getName() + "` ");
				
				if(column.getType() == Types.U_DECIMAL)
				{
				    writer.print("decimal(");
				    writer.print(column.getTypeParameter1());
				    writer.print(",");
				    writer.print(column.getTypeParameter2());
				    writer.print(") unsigned");
				}
				else
				{
				    writer.print(column.getType().name());
    				final int nparams = column.getType().nTypeParameters();
    				if (nparams > 0) {
    					writer.print("(" + column.getTypeParameter1());
    					if (nparams > 1) {
    						writer.print("," + column.getTypeParameter2());
    					}
    					writer.print(")");
    				}
				}
			}
			for (Index index : table.getIndexes()) {
				writer.println(nColumns > 0 ? "," : "");
				writer.print("  ");
				writer.print(index.getConstraint());
				writer.append(" ");
				if (index.getIndexName() != null) {
					writer.print(" `");
					writer.print(index.getIndexName().getName());
					writer.print("` (");
					boolean first = true;
					for (final IndexColumn column : index.getColumns()) {
						if (!first) {
							writer.print(", ");
						}
						writer.print("`");
						writer.print(column.getColumn().getName());
                        writer.print("`");
                        Integer indexedLength = column.getIndexedLength();
                        if (indexedLength != null && indexedLength > 0) {
                            writer.print("(");
                            writer.print(indexedLength);
                            writer.print(")");
                        }
						first = false;
					}
					writer.print(')');
				}
			}
			writer.println();
			writer.println(") engine=" + getStorageEngine() + ";");
		}
	}
	
	public static String dropGroupTableDDL(final GroupTable table) throws Exception {
        return "drop table if exists `" + table.getName().getSchemaName() + "`.`"
                + table.getName().getTableName() + " if exists;";
	}
	
	public static String createGroupTableDDL(final GroupTable table) throws Exception {
	    final StringBuilder sb = new StringBuilder();
        sb.append("create table `" + table.getName().getSchemaName() + "`.`"
                + table.getName().getTableName() + "`(");
        final int nColumns = table.getColumns().size();
        final Column[] columns = new Column[nColumns];
        for (final Column column : table.getColumns()) {
            assert columns[column.getPosition()] == null;
            columns[column.getPosition()] = column;
        }
        for (int position = 0; position < nColumns; position++) {
            final Column column = columns[position];
            sb.append(position > 0 ? "," : "");
            sb.append("  ");
            sb.append("`" + column.getName() + "` ");
            
            if(column.getType() == Types.U_DECIMAL)
            {
                sb.append("decimal(");
                sb.append(column.getTypeParameter1());
                sb.append(",");
                sb.append(column.getTypeParameter2());
                sb.append(") unsigned");
            }
            else
            {
                sb.append(column.getType().name());
                final int nparams = column.getType().nTypeParameters();
                if (nparams > 0) {
                    sb.append("(" + column.getTypeParameter1());
                    if (nparams > 1) {
                        sb.append("," + column.getTypeParameter2());
                    }
                    sb.append(")");
                }
            }
        }
        for (Index index : table.getIndexes()) {
            sb.append(nColumns > 0 ? "," : "");
            sb.append("  ");
            sb.append(index.getConstraint());
            sb.append(" ");
            if (index.getIndexName() != null) {
                sb.append(" `");
                sb.append(index.getIndexName().getName());
                sb.append("` (");
                boolean first = true;
                for (final IndexColumn column : index.getColumns()) {
                    if (!first) {
                        sb.append(", ");
                    }
                    sb.append("`");
                    sb.append(column.getColumn().getName());
                    sb.append("`");
                    Integer indexedLength = column.getIndexedLength();
                    if (indexedLength != null && indexedLength > 0) {
                        sb.append("(");
                        sb.append(indexedLength);
                        sb.append(")");
                    }
                    first = false;
                }
                sb.append(')');
            }
        }
        sb.append(") engine=akibandb;");
        return sb.toString();
	}

	private String getStorageEngine() {
		String engine = System.getProperty("test.db.engine", "akibandb");
		//System.out.println("Generating AIS with engine: " + engine);
		return engine;
	}

	private FakePreparedStatement prepareStatement(final String string) {
		return new FakePreparedStatement(string);
	}

	private void bind(FakePreparedStatement stmt, int position, Object value) {
		stmt.bind(value);
	}

	private PrintWriter writer;
}
