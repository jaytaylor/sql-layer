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

package com.akiban.sql.test;

import com.akiban.sql.StandardException;
import com.akiban.sql.compiler.BooleanNormalizer;
import com.akiban.sql.optimizer.AISTypeComputer;
import com.akiban.sql.optimizer.AISBinder;
import com.akiban.sql.optimizer.BindingNodeFactory;
import com.akiban.sql.optimizer.BoundNodeToString;
import com.akiban.sql.optimizer.Grouper;
import com.akiban.sql.optimizer.SubqueryFlattener;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AkibanInformationSchema;

import java.util.*;

/** Standalone testing. */
public class Tester
{
  enum Action { 
    ECHO, PARSE, CLONE,
    PRINT_TREE, PRINT_SQL, PRINT_BOUND_SQL,
    BIND, COMPUTE_TYPES,
    BOOLEAN_NORMALIZE, FLATTEN_SUBQUERIES,
    GROUP, GROUP_REWRITE  
  }

  List<Action> actions;
  SQLParser parser;
  BoundNodeToString unparser;
  AISBinder binder;
  AISTypeComputer typeComputer;
  BooleanNormalizer booleanNormalizer;
  SubqueryFlattener subqueryFlattener;
  Grouper grouper;

  public Tester() {
    actions = new ArrayList<Action>();
    parser = new SQLParser();
    parser.setNodeFactory(new BindingNodeFactory(parser.getNodeFactory()));
    unparser = new BoundNodeToString();
    typeComputer = new AISTypeComputer();
    booleanNormalizer = new BooleanNormalizer(parser);
    subqueryFlattener = new SubqueryFlattener(parser);
    grouper = new Grouper(parser);
  }

  public void addAction(Action action) {
    actions.add(action);
  }

  public void process(String sql) throws Exception {
    StatementNode stmt = null;
    for (Action action : actions) {
      switch (action) {
      case ECHO:
        System.out.println("=====");
        System.out.println(sql);
        break;
      case PARSE:
        stmt = parser.parseStatement(sql);
        break;
      case CLONE:
        stmt = (StatementNode)parser.getNodeFactory().copyNode(stmt, parser);
        break;
      case PRINT_TREE:
        stmt.treePrint();
        break;
      case PRINT_SQL:
        unparser.setUseBindings(false);
        System.out.println(unparser.toString(stmt));
        break;
      case PRINT_BOUND_SQL:
        unparser.setUseBindings(true);
        System.out.println(unparser.toString(stmt));
        break;
      case BIND:
        binder.bind(stmt);
        break;
      case COMPUTE_TYPES:
        typeComputer.compute(stmt);
        break;
      case BOOLEAN_NORMALIZE:
        stmt = booleanNormalizer.normalize(stmt);
        break;
      case FLATTEN_SUBQUERIES:
        stmt = subqueryFlattener.flatten(stmt);
        break;
      case GROUP:
        grouper.group(stmt);
        break;
      case GROUP_REWRITE:
        grouper.group(stmt);
        grouper.rewrite(stmt);
        break;
      }
    }
  }

  public void setSchema(String sql) throws Exception {
    SchemaDef schemaDef = SchemaDef.parseSchema("use user; " + sql);
    SchemaDefToAis toAis = new SchemaDefToAis(schemaDef, false);
    AkibanInformationSchema ais = toAis.getAis();
    binder = new AISBinder(ais, "user");
  }

  public void addView(String sql) throws Exception {
    binder.addView(new ViewDefinition(sql, parser));
  }

  public static void main(String[] args) throws Exception {
    Tester tester = new Tester();
    tester.addAction(Action.ECHO);
    tester.addAction(Action.PARSE);
    int i = 0;
    while (i < args.length) {
      String arg = args[i++];
      if (arg.startsWith("-")) {
        if ("-tree".equals(arg))
          tester.addAction(Action.PRINT_TREE);
        else if ("-print".equals(arg))
          tester.addAction(Action.PRINT_SQL);
        else if ("-print-bound".equals(arg))
          tester.addAction(Action.PRINT_BOUND_SQL);
        else if ("-clone".equals(arg))
          tester.addAction(Action.CLONE);
        else if ("-bind".equals(arg)) {
          tester.setSchema(args[i++]);
          tester.addAction(Action.BIND);
        }
        else if ("-view".equals(arg))
          tester.addView(args[i++]);
        else if ("-types".equals(arg))
          tester.addAction(Action.COMPUTE_TYPES);
        else if ("-boolean".equals(arg))
          tester.addAction(Action.BOOLEAN_NORMALIZE);
        else if ("-flatten".equals(arg))
          tester.addAction(Action.FLATTEN_SUBQUERIES);
        else if ("-group".equals(arg))
          tester.addAction(Action.GROUP);
        else if ("-group-rewrite".equals(arg))
          tester.addAction(Action.GROUP_REWRITE);
        else
          throw new Exception("Unknown switch: " + arg);
      }
      else {
        try {
          tester.process(arg);
        }
        catch (StandardException ex) {
          System.out.flush();
          ex.printStackTrace();
        }
      }
    }
  }
}
