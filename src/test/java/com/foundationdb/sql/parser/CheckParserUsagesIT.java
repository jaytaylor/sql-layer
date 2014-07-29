package com.foundationdb.sql.parser;

import org.hamcrest.core.StringEndsWith;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.reflections.Reflections;
import org.reflections.scanners.AbstractScanner;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by scott on 7/29/14.
 */
public class CheckParserUsagesIT {

    private static Set<Class<? extends QueryTreeNode>> queryTreeNodes;
    private static Collection<String> sqlLayerClassPaths;

    @BeforeClass
    public static void getParserClasses() {
        Reflections reflections = new Reflections("com.foundationdb.sql.parser");
        queryTreeNodes = reflections.getSubTypesOf(QueryTreeNode.class);
    }

    @BeforeClass
    public static void getSqlLayerClassNames() throws Exception {
        sqlLayerClassPaths = getClassesInPackage("com.foundationdb.sql", "com.foundationdb.sql.Main");
        System.out.println(sqlLayerClassPaths);
    }

    private static Collection<String> getClassesInPackage(String packageName, String sampleClass) {
        String sampleClassPathSuffix = sampleClass.replaceAll("\\.", "/") + ".class";
        String sampleClassPath = CheckParserUsagesIT.class.getClassLoader().getResource(sampleClassPathSuffix).getPath();
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

    @Test
    public void checkSqlLayerClassCount() {
        assertEquals(5, sqlLayerClassPaths.size());
    }
}


