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

package com.akiban.ais.model;

import java.util.*;

import com.akiban.ais.model.validation.AISInvariants;

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
        Collection<Column> columns = new HashSet<Column>();
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
