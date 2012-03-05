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

package com.akiban.sql.optimizer;

import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.sql.compiler.ASTTransformTestBase;
import com.akiban.sql.compiler.BooleanNormalizer;
import com.akiban.sql.compiler.TypeComputer;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index.JoinType;
import com.akiban.server.service.functions.FunctionsRegistryImpl;

import org.junit.Before;
import org.junit.Ignore;

import java.io.File;

@Ignore
public class OptimizerTestBase extends ASTTransformTestBase
{
    protected OptimizerTestBase(String caseName, String sql, 
                                String expected, String error) {
        super(caseName, sql, expected, error);
    }

    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + OptimizerTestBase.class.getPackage().getName().replace('.', '/'));
    public static final String DEFAULT_SCHEMA = "user";

    // Base class has all possible transformers for convenience.
    protected AISBinder binder;
    protected TypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected DistinctEliminator distinctEliminator;

    @Before
    public void makeTransformers() throws Exception {
        parser = new SQLParser();
        parser.setNodeFactory(new BindingNodeFactory(parser.getNodeFactory()));
        unparser = new BoundNodeToString();
        typeComputer = new FunctionsTypeComputer(new FunctionsRegistryImpl());
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        distinctEliminator = new DistinctEliminator(parser);
    }

    public static AkibanInformationSchema parseSchema(File schema) throws Exception {
        String sql = fileContents(schema);
        SchemaFactory schemaFactory = new SchemaFactory(DEFAULT_SCHEMA);
        return schemaFactory.ais(sql);
    }

    protected AkibanInformationSchema loadSchema(File schema) throws Exception {
        AkibanInformationSchema ais = parseSchema(schema);
        binder = new AISBinder(ais, DEFAULT_SCHEMA);
        return ais;
    }

    protected void loadView(File view) throws Exception {
        String sql = fileContents(view);
        binder.addView(new ViewDefinition(sql, parser));
    }

}
