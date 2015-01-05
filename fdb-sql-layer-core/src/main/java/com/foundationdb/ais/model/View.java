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

package com.foundationdb.ais.model;

import java.util.*;

import com.foundationdb.ais.model.validation.AISInvariants;

public class View extends Columnar
{
    public static View create(AkibanInformationSchema ais,
                              String schemaName, String viewName,
                              String definition, Properties definitionProperties,
                              Map<TableName,Collection<String>> tableColumnReferences) {
        View view = new View(ais, schemaName, viewName);
        view.setDefinition(definition, definitionProperties);
        view.setTableColumnReferences(tableColumnReferences);
        ais.addView(view);
        return view;
    }

    @Override
    public boolean isView() {
        return true;
    }

    public View(AkibanInformationSchema ais, String schemaName, String viewName)
    {
        super(ais, schemaName, viewName);
    }

    public String getDefinition() {
        return definition;
    }

    public Properties getDefinitionProperties() {
        return definitionProperties;
    }

    protected void setDefinition(String definition, Properties definitionProperties) {
        this.definition = definition;
        this.definitionProperties = definitionProperties;
    }

    public Map<TableName,Collection<String>> getTableColumnReferences() {
        return tableColumnReferences;
    }

    protected void setTableColumnReferences(Map<TableName,Collection<String>> tableColumnReferences) {
        this.tableColumnReferences = tableColumnReferences;
    }

    public Collection<TableName> getTableReferences() {
        return tableColumnReferences.keySet();
    }

    public Collection<Column> getTableColumnReferences(Columnar table) {
        Collection<String> colnames = tableColumnReferences.get(table.getName());
        if (colnames == null) return null;
        Collection<Column> columns = new HashSet<>();
        for (String colname : colnames) {
            columns.add(table.getColumn(colname));
        }
        return columns;
    }

    public boolean referencesTable(Columnar table) {
        return tableColumnReferences.containsKey(table.getName());
    }

    public boolean referencesColumn(Column column) {
        Collection<String> entry = tableColumnReferences.get(column.getColumnar().getName());
        return ((entry != null) && entry.contains(column.getName()));
    }

    // State
    private String definition;
    private Properties definitionProperties;
    private Map<TableName,Collection<String>> tableColumnReferences;
}
