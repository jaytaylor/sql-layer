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

package com.foundationdb.sql.parser;

import org.hamcrest.core.StringEndsWith;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;


public class CheckParserUsagesDT {

    private static Logger logger = LoggerFactory.getLogger(CheckParserUsagesDT.class);
    private static Logger csvLogger = LoggerFactory.getLogger(CheckParserUsagesDT.class.getName() + ".csv");
    private static Logger sqlLogger = LoggerFactory.getLogger(CheckParserUsagesDT.class.getName() + ".sql");
    private static Logger codeLogger = LoggerFactory.getLogger(CheckParserUsagesDT.class.getName() + ".code");

    private static Set<Class<? extends QueryTreeNode>> queryTreeNodes;
    private static Collection<String> sqlLayerClassPaths;
    private PropertyFinder finder;

    @BeforeClass
    public static void getParserClasses() {
        Reflections reflections = new Reflections("com.foundationdb.sql.parser");
        queryTreeNodes = reflections.getSubTypesOf(QueryTreeNode.class);
        // Note: queryTreeNode is not counted here.
    }

    @BeforeClass
    public static void getSqlLayerClassNames() throws Exception {
        sqlLayerClassPaths = getClassesInPackage("com.foundationdb.sql", "com.foundationdb.sql.Main");
    }

    private static Collection<String> getClassesInPackage(String packageName, String sampleClass) {
        String sampleClassPathSuffix = sampleClass.replaceAll("\\.", "/") + ".class";
        String sampleClassPath = CheckParserUsagesDT.class.getClassLoader().getResource(sampleClassPathSuffix).getPath();
        assertThat(sampleClassPath, new StringEndsWith(sampleClassPathSuffix));
        String packagePath = sampleClassPath.substring(0,sampleClassPath.length()-sampleClassPathSuffix.length()) +
                packageName.replaceAll("\\.", "/");
        return getAllClassesInDirectory(new File(packagePath));
    }

    private static Collection<String> getAllClassesInDirectory(File directory) {
        Collection<String> result = new HashSet<>();
        for (File file : directory.listFiles()) {
            if (file.isDirectory())
            {
                result.addAll(getAllClassesInDirectory(file));
            } else if (file.isFile() && file.getName().endsWith(".class")) {
                result.add(file.getAbsolutePath());
            }
        }
        return result;
    }

    @Before
    public void initializeFinder() throws Exception {
        finder = new PropertyFinder();
        for (Class<? extends QueryTreeNode> nodeClass : queryTreeNodes) {
            try {
                ClassReader reader = new ClassReader(nodeClass.getName());
                reader.accept(finder, 0);
            } catch (IOException e) {
                throw new Exception("Could not open class to scan: " + nodeClass.getName(), e);
            }
        }
        // Remove any base class methods here, before they get propagated down
        finder.getNodes().get("com/foundationdb/sql/parser/DDLStatementNode")
                .removeMethod("getRelativeName", "()Ljava/lang/String;")
                .removeMethod("getFullName", "()Ljava/lang/String;");
        finder.finalizeState();
        // Remove any concrete class methods here, base class methods have already been propagated down
        removeTemporarilyIgnoredUnused(finder);
    }

    /**
     * After running this test for the first time we found a lot that were ignored.
     * They look ok enough that we will table this until a later point in time.
     * Ignoring them here means that we haven't checked them fully. Once we get
     * a chance we should go through them and confirm and then either remove them
     * or put them in a different method
     * @param finder
     */
    private static void removeTemporarilyIgnoredUnused(PropertyFinder finder) {
        finder.getNodes().get("com/foundationdb/sql/parser/CopyStatementNode")
                .removeMethod("getSubquery","()Lcom/foundationdb/sql/parser/SubqueryNode;");
        finder.getNodes().get("com/foundationdb/sql/parser/CallStatementNode")
                .removeMethod("getResultSetNode","()Lcom/foundationdb/sql/parser/ResultSetNode;");
        finder.getNodes().get("com/foundationdb/sql/parser/ResultColumnList")
                .removeMethod("getOrderByColumn","(I)Lcom/foundationdb/sql/parser/ResultColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/ResultColumn")
                .removeMethod("getTableNameObject","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getVirtualColumnId","()I")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/AndNode")
                .removeMethod("getOperator","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getMethodName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/SubqueryNode")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/SimpleCaseNode")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getCaseOperands","()Lcom/foundationdb/sql/parser/ValueNodeList;")
                .removeMethod("getResultValues","()Lcom/foundationdb/sql/parser/ValueNodeList;")
                .removeMethod("getTableName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/ParameterNode")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/GroupByColumn")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getColumnPosition","()I");
        finder.getNodes().get("com/foundationdb/sql/parser/CreateSequenceNode")
                .removeMethod("getSequenceName","()Lcom/foundationdb/sql/parser/TableName;");
        finder.getNodes().get("com/foundationdb/sql/parser/ConditionalNode")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getThenElseList","()Lcom/foundationdb/sql/parser/ValueNodeList;");
        finder.getNodes().get("com/foundationdb/sql/parser/AlterTableRenameNode")
                .removeMethod("getName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/CastNode")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;");
        finder.getNodes().get("com/foundationdb/sql/parser/CursorNode")
                .removeMethod("getScanIsolationLevel","()Lcom/foundationdb/sql/parser/IsolationLevel;")
                .removeMethod("getUpdatableColumns","()Ljava/util/List;")
                .removeMethod("getName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/UpdateNode")
                .removeMethod("getTargetTableName","()Lcom/foundationdb/sql/parser/TableName;");
        finder.getNodes().get("com/foundationdb/sql/parser/ColumnDefinitionNode")
                .removeMethod("getAutoinc_create_or_modify_Start_Increment","()J")
                .removeMethod("getGenerationClauseNode","()Lcom/foundationdb/sql/parser/GenerationClauseNode;")
                .removeMethod("getName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/CurrentSequenceNode")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getColumnName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/RowResultSetNode")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getCorrelationName","()Ljava/lang/String;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getOrigTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getTableName","()Lcom/foundationdb/sql/parser/TableName;");
        finder.getNodes().get("com/foundationdb/sql/parser/HalfOuterJoinNode")
                .removeMethod("getCorrelationName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getUsingClause","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getJoinClause","()Lcom/foundationdb/sql/parser/ValueNode;")
                .removeMethod("getLogicalLeftResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getLogicalRightResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getOrigTableName","()Lcom/foundationdb/sql/parser/TableName;");
        finder.getNodes().get("com/foundationdb/sql/parser/CoalesceFunctionNode")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/ColumnReference")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/ModifyColumnNode")
                .removeMethod("getName","()Ljava/lang/String;")
                .removeMethod("getAutoincrementIncrement","()J")
                .removeMethod("getGenerationClauseNode","()Lcom/foundationdb/sql/parser/GenerationClauseNode;")
                .removeMethod("getDefaultNode","()Lcom/foundationdb/sql/parser/DefaultNode;");
        finder.getNodes().get("com/foundationdb/sql/parser/DropTableNode")
                .removeMethod("getDropBehavior","()I");
        finder.getNodes().get("com/foundationdb/sql/parser/BooleanConstantNode")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getValue","()Ljava/lang/Object;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/NextSequenceNode")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;");
        finder.getNodes().get("com/foundationdb/sql/parser/TableElementNode")
                .removeMethod("getName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/RowsResultSetNode")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getOrigTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getCorrelationName","()Ljava/lang/String;")
                .removeMethod("getExposedName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/InListOperatorNode")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/CreateIndexNode")
                .removeMethod("getIndexName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getProperties","()Ljava/util/Properties;")
                .removeMethod("getJoinType","()Lcom/foundationdb/sql/parser/JoinNode$JoinType;");
        finder.getNodes().get("com/foundationdb/sql/parser/UntypedNullConstantNode")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getValue","()Ljava/lang/Object;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;");
        finder.getNodes().get("com/foundationdb/sql/parser/BetweenOperatorNode")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;");
        finder.getNodes().get("com/foundationdb/sql/parser/IsNode")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getMethodName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getOperator","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;");
        finder.getNodes().get("com/foundationdb/sql/parser/DropSchemaNode")
                .removeMethod("getObjectName","()Lcom/foundationdb/sql/parser/TableName;");
        finder.getNodes().get("com/foundationdb/sql/parser/UnionNode")
                .removeMethod("getCorrelationName","()Ljava/lang/String;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getOrigTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getExposedName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/OrderByList")
                .removeMethod("get","(I)Lcom/foundationdb/sql/parser/QueryTreeNode;")
                .removeMethod("getOrderByColumn","(I)Lcom/foundationdb/sql/parser/OrderByColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/FKConstraintDefinitionNode")
                .removeMethod("getVerifyType","()Lcom/foundationdb/sql/parser/ConstraintDefinitionNode$ConstraintType;")
                .removeMethod("getProperties","()Ljava/util/Properties;");
        finder.getNodes().get("com/foundationdb/sql/parser/AlterDropIndexNode")
                .removeMethod("getName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/CreateSchemaNode")
                .removeMethod("getDefaultCharacterAttributes","()Lcom/foundationdb/sql/types/CharacterTypeAttributes;")
                .removeMethod("getObjectName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getAuthorizationID","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/CurrentDatetimeOperatorNode")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/IntersectOrExceptNode")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getCorrelationName","()Ljava/lang/String;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getOrigTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getOpType","()Lcom/foundationdb/sql/parser/IntersectOrExceptNode$OpType;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getExposedName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/IndexDefinitionNode")
                .removeMethod("getIndexColumnList","()Lcom/foundationdb/sql/parser/IndexColumnList;")
                .removeMethod("getStorageFormat","()Lcom/foundationdb/sql/parser/StorageFormatNode;");
        finder.getNodes().get("com/foundationdb/sql/parser/CreateViewNode")
                .removeMethod("getOrderByList","()Lcom/foundationdb/sql/parser/OrderByList;")
                .removeMethod("getOffset","()Lcom/foundationdb/sql/parser/ValueNode;")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getCheckOption","()I")
                .removeMethod("getFetchFirst","()Lcom/foundationdb/sql/parser/ValueNode;")
                .removeMethod("getQueryExpression","()Ljava/lang/String;")
                .removeMethod("getParsedQueryExpression","()Lcom/foundationdb/sql/parser/ResultSetNode;");
        finder.getNodes().get("com/foundationdb/sql/parser/AllResultColumn")
                .removeMethod("getColumnPosition","()I")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getExpression","()Lcom/foundationdb/sql/parser/ValueNode;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getReference","()Lcom/foundationdb/sql/parser/ColumnReference;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getFullTableName","()Ljava/lang/String;")
                .removeMethod("getVirtualColumnId","()I")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/GroupConcatNode")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getAggregateName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getOperand","()Lcom/foundationdb/sql/parser/ValueNode;")
                .removeMethod("getOperator","()Ljava/lang/String;")
                .removeMethod("getMethodName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;");
        finder.getNodes().get("com/foundationdb/sql/parser/BinaryOperatorNode")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getOperator","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getSchemaName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/FromBaseTable")
                .removeMethod("getIndexHints","()Lcom/foundationdb/sql/parser/IndexHintList;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;")
                .removeMethod("getExposedName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/IsNullNode")
                .removeMethod("getOperator","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getTableName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/InsertNode")
                .removeMethod("getTargetProperties","()Ljava/util/Properties;");
        finder.getNodes().get("com/foundationdb/sql/parser/AlterTableNode")
                .removeField("defragment")
                .removeField("purge")
                .removeField("compressTable")
                .removeField("truncateEndOfTable")
                .removeField("behavior")
                .removeField("sequential")
                .removeMethod("getBehavior","()I")
                .removeMethod("getChangeType","()I");
        finder.getNodes().get("com/foundationdb/sql/parser/DefaultNode")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;");
        finder.getNodes().get("com/foundationdb/sql/parser/StaticMethodCallNode")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getJSQLType","()Lcom/foundationdb/sql/types/JSQLType;")
                .removeMethod("getJavaClassName","()Ljava/lang/String;")
                .removeMethod("getJavaTypeName","()Ljava/lang/String;")
                .removeMethod("getPrimitiveTypeName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/SpecialFunctionNode")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/SelectNode")
                .removeMethod("getWindows","()Lcom/foundationdb/sql/parser/WindowList;")
                .removeMethod("getCacheHint","()Ljava/lang/Boolean;");
        finder.getNodes().get("com/foundationdb/sql/parser/TernaryOperatorNode")
                .removeMethod("getOperator","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/DropSequenceNode")
                .removeMethod("getDropBehavior","()I");
        finder.getNodes().get("com/foundationdb/sql/parser/AggregateNode")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getMethodName","()Ljava/lang/String;")
                .removeMethod("getOperator","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/ConstraintDefinitionNode")
                .removeMethod("getProperties","()Ljava/util/Properties;");
        finder.getNodes().get("com/foundationdb/sql/parser/OrderByColumn")
                .removeMethod("getColumnPosition","()I");
        finder.getNodes().get("com/foundationdb/sql/parser/RenameNode")
                .removeMethod("getNewObjectName","()Ljava/lang/String;")
                .removeMethod("getOldObjectName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/DeleteNode")
                .removeMethod("getTargetTableName","()Lcom/foundationdb/sql/parser/TableName;");
        finder.getNodes().get("com/foundationdb/sql/parser/RowConstructorNode")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getDepth","()I")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTableName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/NumericConstantNode")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;");
        finder.getNodes().get("com/foundationdb/sql/parser/UnaryOperatorNode")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getOperator","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/SQLToJavaValueNode")
                .removeMethod("getJSQLType","()Lcom/foundationdb/sql/types/JSQLType;")
                .removeMethod("getJavaTypeName","()Ljava/lang/String;")
                .removeMethod("getPrimitiveTypeName","()Ljava/lang/String;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;");
        finder.getNodes().get("com/foundationdb/sql/parser/JavaToSQLValueNode")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getSchemaName","()Ljava/lang/String;");
        finder.getNodes().get("com/foundationdb/sql/parser/GroupByList")
                .removeMethod("get","(I)Lcom/foundationdb/sql/parser/QueryTreeNode;")
                .removeMethod("getGroupByColumn","(I)Lcom/foundationdb/sql/parser/GroupByColumn;");
        finder.getNodes().get("com/foundationdb/sql/parser/FromSubquery")
                .removeMethod("getCorrelationName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getOrigTableName","()Lcom/foundationdb/sql/parser/TableName;");
        finder.getNodes().get("com/foundationdb/sql/parser/JoinNode")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getOrigTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getCorrelationName","()Ljava/lang/String;")
                .removeMethod("getExposedName","()Ljava/lang/String;")
                .removeMethod("getLeftmostResultSet","()Lcom/foundationdb/sql/parser/ResultSetNode;")
                .removeMethod("getTableName","()Lcom/foundationdb/sql/parser/TableName;")
                .removeMethod("getResultColumns","()Lcom/foundationdb/sql/parser/ResultColumnList;");
        finder.getNodes().get("com/foundationdb/sql/parser/ExplicitCollateNode")
                .removeMethod("getType","()Lcom/foundationdb/sql/types/DataTypeDescriptor;")
                .removeMethod("getSourceResultColumn","()Lcom/foundationdb/sql/parser/ResultColumn;")
                .removeMethod("getColumnName","()Ljava/lang/String;")
                .removeMethod("getSchemaName","()Ljava/lang/String;")
                .removeMethod("getTableName","()Ljava/lang/String;")
                .removeMethod("getTypeId","()Lcom/foundationdb/sql/types/TypeId;");
    }

    @Test
    public void testAllReferencedClassesHaveReferencedGetters() throws Exception {
        UsageClassVisitor checker = new UsageClassVisitor(finder.getNodes());
        int fullyUsed = 0;
        int total = 0;
        Iterator<Map.Entry<String, NodeClass>> iterator = finder.getNodes().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, NodeClass> nodeClass = iterator.next();
            if (nodeClass.getValue().fullyUsed()) {
                fullyUsed++;
                iterator.remove();
            }
            total++;
        }
        assertThat(fullyUsed, lessThan(total));
        for (String usageClass : sqlLayerClassPaths) {
            try {
                ClassReader reader = new ClassReader(new FileInputStream(usageClass));
                reader.accept(checker, 0);
            } catch (IOException e) {
                throw new Exception("Failed to check against class", e);
            }
        }
        StringBuilder sql = new StringBuilder("Sql\n");
        StringBuilder csv = new StringBuilder("\n");
        logHeaderInfo(sql, csv);
        Collection<String> unused = new TreeSet<>();
        for (NodeClass nodeClass : finder.getNodes().values()) {
            if (nodeClass.isReferenced && nodeClass.isConcrete()) {
                String name = nodeClass.getJavaName();
                codeLogger.debug("finder.getNodes().get(\"{}\")",nodeClass.getName());
                for (NodeClass.Field field : nodeClass.fields) {
                    if (!field.isReferenced) {
                        codeLogger.debug(".removeField(\"{}\")", field.name);
                        unused.add(name + "." + field.name);
                    }
                    logMember(name, field, sql, csv);
                }
                for (NodeClass.Method method : nodeClass.methods) {
                    if (!method.isReferenced) {
                        codeLogger.debug(".removeMethod(\"{}\",\"{}\")",method.name, method.descriptor);
                        unused.add(method.getJavaString(name) + " -- " + method.descriptor);
                    }
                    logMember(name, method, sql, csv);
                }
                codeLogger.debug(";");
            }
        }
        if (sqlLogger.isDebugEnabled()) {
            sqlLogger.debug(sql.toString());
        }
        if (csvLogger.isDebugEnabled()) {
            csvLogger.debug(csv.toString());
        }
        assertThat(unused, empty());
    }

    public void logHeaderInfo(StringBuilder sql, StringBuilder csv) {
        if (csvLogger.isDebugEnabled()) {
            csv.append("DeclaredType,SubType,PropertyType,IsReferenced,Name,Java Declaration,Code To Remove\n");
        }
        if (sqlLogger.isDebugEnabled()) {
            // helpful sql:
            // SELECT declaredtype,name,cnr,cr FROM
            //     (SELECT declaredtype,name,count(*) as cr FROM methods
            //          WHERE IsReferenced IS TRUE GROUP BY declaredtype,name) AS ReferencedCounts
            //     RIGHT OUTER JOIN
            //     (SELECT declaredtype,name,COUNT(*) as cnr FROM methods
            //          WHERE IsReferenced IS FALSE GROUP BY declaredtype,name) AS UnReferencedCounts
            //     USING(declaredtype,name) ORDER BY cnr;
            //
            // SELECT declaredtype,name,java,removal FROM methods WHERE declaredtype = subtype AND isreferenced IS FALSE;
            sql.append("CREATE TABLE fields (DeclaredType VARCHAR(100),SubType VARCHAR(100)," +
                    "PropertyType VARCHAR(25),IsReferenced BOOLEAN,Name VARCHAR(100));\n");
            sql.append("CREATE TABLE methods (DeclaredType VARCHAR(100),SubType VARCHAR(100)," +
                    "PropertyType VARCHAR(25),IsReferenced BOOLEAN,Name VARCHAR(100)," +
                    "Java VARCHAR(200),Removal VARCHAR(200));\n");
        }
    }

    public void logMember(String typeName, NodeClass.Member member, StringBuilder sql, StringBuilder csv) {
        if (csvLogger.isDebugEnabled()) {
            csv.append(member.csvString(typeName));
            csv.append("\n");
        }
        if (sqlLogger.isDebugEnabled()) {
            sql.append(member.sqlInsertStatement("methods", typeName));
            sql.append("\n");
        }
    }

    /**
     * Finds all fields and methods adding them to a list of nodes
     */
    public static class PropertyFinder extends ClassVisitor {

        private Map<String, NodeClass> nodes;
        private NodeClass currentClass;

        public PropertyFinder() {
            super(Opcodes.ASM5);
            nodes = new HashMap<>();
        }

        public Map<String, NodeClass> getNodes() {
            return nodes;
        }

        @Override
        public void visit(int version, int access, String name,
                          String signature, String superName, String[] interfaces) {

            currentClass = new NodeClass(name, superName,
                    (access & Opcodes.ACC_ABSTRACT) > 0, (access & Opcodes.ACC_INTERFACE) > 0);
            nodes.put(name, currentClass);
        }

        @Override
        public FieldVisitor visitField(int access, String name, String desc,
                                       String signature, Object value) {
            currentClass.addField(access, name);
            return null;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name,
                                         String desc, String signature, String[] exceptions) {
            currentClass.addMethod(access, name, desc);
            return null;
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            for (NodeClass node : nodes.values()) {
                stringBuilder.append(node.toString());
                stringBuilder.append("\n");
            }
            return stringBuilder.toString();
        }

        public void finalizeState() {
            for (NodeClass nodeClass : nodes.values()) {
                nodeClass.incorporateBaseClass(nodes);
            }
        }
    }

    /**
     * Marks all nodes & there members as referenced, if they are.
     */
    public static class UsageClassVisitor extends ClassVisitor{

        private Map<String, NodeClass> nodes;

        public UsageClassVisitor(Map<String, NodeClass> nodes) {
            super(Opcodes.ASM5);
            this.nodes = nodes;
        }

        private void markNodesAsVisited(String descriptor, int maxCount) {
            Collection<String> referencedTypes = getParameterAndReturnTypes(descriptor);
            assertThat(referencedTypes.size(), lessThanOrEqualTo(maxCount));
            for (String referencedType : referencedTypes) {
                if (nodes.containsKey(referencedType)) {
                    nodes.get(referencedType).reference();
                }
            }
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            if (nodes.containsKey(superName)) {
                throw new RuntimeException("Class " + name + " extends " + superName);
            }
        }

        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            markNodesAsVisited(desc, 1);
            return null;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            markNodesAsVisited(desc, Integer.MAX_VALUE);
            return new UsageMethodVisitor();
        }

        private Collection<String> getParameterAndReturnTypes(String descriptor) {
            Set<String> types = new HashSet<>();
            Type type = Type.getType(descriptor);
            types.add(NodeClass.typeToString(type.getReturnType()));
            if (type.getSort() == Type.METHOD) {
                for (Type argumentType : type.getArgumentTypes()) {
                    types.add(NodeClass.typeToString(argumentType));
                }
            }
            return types;
        }

        private class UsageMethodVisitor extends MethodVisitor{

            public UsageMethodVisitor() {
                super(Opcodes.ASM5);
            }

            @Override
            public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs) {
                throw new RuntimeException("Unexpected Dynamic Instruction " + name);
            }

            @Override
            public void visitFieldInsn(int opcode, String owner, String name, String desc) {
                if (nodes.containsKey(owner)) {
                    nodes.get(owner).usedField(name);
                }
            }

            @Override
            public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
                markNodesAsVisited(desc, 1);
            }

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
                if (nodes.containsKey(owner)) {
                    nodes.get(owner).referenceMethod(name, desc);
                }
            }

            @Override
            @Deprecated // just like parent method
            @SuppressWarnings("deprecated")
            public void visitMethodInsn(int opcode, String owner, String name, String desc) {
                this.visitMethodInsn(opcode, owner, name, desc, false);
            }

            @Override
            public void visitTypeInsn(int opcode, String type) {
                if (nodes.containsKey(type)) {
                    nodes.get(type).reference();
                }
            }
        }
    }

    public static class NodeClass {
        public String name;
        public String baseClassName;
        public NodeClass baseClass;
        public Set<Field> fields;
        private Set<Method> methods;
        private boolean isAbstract;
        private boolean isInterface;
        private boolean isReferenced;

        public NodeClass(String name, String baseClassName, boolean isAbstract, boolean isInterface) {
            this.name = name;
            this.baseClassName = baseClassName;
            this.isAbstract = isAbstract;
            this.isInterface = isInterface;
            fields = new HashSet<>();
            methods = new HashSet<>();
        }

        public String getName() {
            return name;
        }

        public String getJavaName() {
            return name.replaceAll("/", ".");
        }

        public void addField(int access, String fieldName) {
            if ((access & Opcodes.ACC_PUBLIC) > 0) {
                if ((access & Opcodes.ACC_STATIC) == 0) {
                    fields.add(new Field(this.name, fieldName));
                    if (logger.isWarnEnabled()) {
                        logger.warn(getJavaName() + " has a public field: " + fieldName);
                    }
                }
            }
        }

        public Method addMethod(int access, String name, String descriptor) {
            if ((access & Opcodes.ACC_PUBLIC) > 0) {
                if ((access & Opcodes.ACC_STATIC) == 0) {
                    if (name.startsWith("get")) {
                        Method member = new Method(this.name, name, descriptor);
                        methods.add(member);
                        return member;
                    }
                }
            }
            return null;
        }

        public void incorporateBaseClass(Map<String, NodeClass> nodeClasses) {
            if (baseClass == null) {
                if (nodeClasses.containsKey(baseClassName)) {
                    baseClass = nodeClasses.get(baseClassName);
                    baseClass.incorporateBaseClass(nodeClasses);
                    for (Field field : baseClass.fields){
                        fields.add(new Field(field));
                    }
                    for (Method method : baseClass.methods) {
                        methods.add(new Method(method));
                    }
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(getJavaName());
            stringBuilder.append(": ");
            for (Field field : fields) {
                stringBuilder.append(field);
                stringBuilder.append(", ");
            }
            for (Method member : methods) {
                stringBuilder.append(member);
                stringBuilder.append(", ");
            }
            return stringBuilder.toString();
        }

        public void referenceMethod(String name, String desc) {
            if (name.startsWith("get")) {
                for (Method method : methods) {
                    if (method.equals(name, desc)) {
                        method.reference();
                    }
                }
            }
        }

        public void usedField(String name) {
            for (Field field : fields) {
                if (field.name.equals(name)) {
                    field.reference();
                }
            }
        }

        public boolean fullyUsed() {
            return methods.size() == 0 && fields.size() == 0;
        }

        public void reference() {
            isReferenced = true;
        }

        public NodeClass removeField(String fieldName) {
            Iterator<Field> iterator = fields.iterator();
            while (iterator.hasNext()) {
                if (Objects.equals(iterator.next().name, fieldName)) {
                    iterator.remove();
                    break;
                }
            }
            return this;
        }

        public NodeClass removeMethod(String name, String descriptor) {
            Iterator<Method> iterator = methods.iterator();
            while (iterator.hasNext()) {
                if (iterator.next().equals(name, descriptor)) {
                    iterator.remove();
                    break;
                }
            }
            return this;
        }

        public boolean isConcrete() {
            return !isAbstract && !isInterface;
        }

        public static String typeToString(Type type) {
            String className = type.getClassName();
            if (type.getSort() == Type.OBJECT) {
                className = className.substring(className.lastIndexOf('.')+1);
            }
            return className;
        }

        public static class Method extends Member {

            private final String descriptor;

            public Method(String className, String name, String descriptor) {
                super(className, name);
                this.descriptor = descriptor;
            }

            public Method(Method method) {
                super(method);
                this.descriptor = method.descriptor;
            }

            @Override
            public String toString() {
                return name + " " + descriptor;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj instanceof Method) {
                    Method method = (Method) obj;
                    return method.name.equals(this.name) && method.descriptor.equals(this.descriptor);
                }
                return false;
            }

            public boolean equals(String name, String descriptor) {
                return this.name.equals(name) && this.descriptor.equals(descriptor);
            }

            public String getJavaString(String className) {
                StringBuilder stringBuilder = new StringBuilder();
                Type type = Type.getType(descriptor);
                stringBuilder.append(typeToString(type.getReturnType()));
                stringBuilder.append(" ");
                if (className != null) {
                    stringBuilder.append(className);
                    stringBuilder.append(".");
                }
                stringBuilder.append(name);
                stringBuilder.append("(");
                for (Type argumentType : type.getArgumentTypes()) {
                    stringBuilder.append(typeToString(argumentType));
                    stringBuilder.append(", ");
                }
                if (type.getArgumentTypes().length > 0) {
                    stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length() - 1);
                }
                stringBuilder.append(")");
                return stringBuilder.toString();
            }

            @Override
            public String csvString(String forType) {
                return super.csvString(forType) + ",\"" + getJavaString(null) + "\",\""
                                + ".removeMethod(\"\"" + name + "\"\", \"\"" + descriptor + ")\"";
            }

            @Override
            public String sqlInsertStatement(String table, String forType) {
                return "INSERT INTO \"" + table +
                        "\" (" + sqlColumnNames() + ",Java,Removal) VALUES (" + sqlColumnValues(forType) + ",'" +
                        getJavaString(null) + "','" + ".removeMethod(\"" + name + "\", \"" + descriptor + "\")');";
            }

            @Override
            protected String getPropertyType() {
                return "Method";
            }
        }

        public static class Field extends Member {

            public Field(String className, String name) {
                super(className, name);
            }

            @Override
            public String sqlInsertStatement(String table, String forType) {
                return "INSERT INTO \"" + table +
                        "\" (" + sqlColumnNames() + ") VALUES (" + sqlColumnValues(forType) + ");";
            }

            @Override
            protected String getPropertyType() {
                return "Field";
            }

            public Field(Field field) {
                super(field);
            }

            @Override
            public String toString() {
                return name;
            }
        }

        public static abstract class Member {
            protected final String className;
            protected final String name;
            boolean isReferenced;

            public Member(String className, String name) {
                this.className = className;
                this.name = name;
            }

            public Member(Member member) {
                className = member.className;
                name = member.name;
                isReferenced = member.isReferenced;
            }

            public void reference() {
                isReferenced = true;
            }

            public String csvString(String forType) {
                return getClassName() + "," + forType + "," + getPropertyType() + "," + isReferenced + "," + name;
            }

            public abstract String sqlInsertStatement(String table, String forType);

            protected String sqlColumnNames() {
                return "DeclaredType,SubType,PropertyType,IsReferenced,Name";
            }

            protected String sqlColumnValues(String forType) {
                return "'" + getClassName() + "','" + forType + "','" + getPropertyType() + "'," +
                        isReferenced + ",'" + name + "'";
            }

            protected abstract String getPropertyType();

            public String getClassName() {
                return className.replaceAll("/", ".");
            }
        }
    }


}


