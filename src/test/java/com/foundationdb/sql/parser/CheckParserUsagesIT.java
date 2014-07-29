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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class CheckParserUsagesIT {

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

    @Before
    public void initializeFinder() {
        finder = new PropertyFinder();
        for (Class<? extends QueryTreeNode> nodeClass : queryTreeNodes) {
            try {
                ClassReader reader = new ClassReader(nodeClass.getName());
                reader.accept(finder, 0);
            } catch (IOException e) {
                System.err.println("Could not open class to scan: " + nodeClass.getName());
                System.exit(1);
            }
        }
        // Remove any base class methods here, before they get propagated down
        finder.getNodes().get("com/foundationdb/sql/parser/DDLStatementNode")
                .removeMethod("getRelativeName", "()Ljava/lang/String;")
                .removeMethod("getFullName", "()Ljava/lang/String;");
        finder.finalizeState();
        // Remove any concrete class methods here, base class methods have already been propagated down
    }

    @Test
    public void testAllReferencedClassesHaveReferencedGetters() {
        PropertyChecker checker = new PropertyChecker(finder.getNodes());
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
        assertTrue(fullyUsed + " < " + total, fullyUsed < total);
        System.out.println(fullyUsed + " / " + total);
        for (String usageClass : sqlLayerClassPaths) {
            try {
                ClassReader reader = new ClassReader(new FileInputStream(usageClass));
                reader.accept(checker, 0);
            } catch (IOException e) {
                System.err.println("Failed to check against class");
                e.printStackTrace();
                System.exit(1);
            }
        }
        int fullyUsed2 = 0;
        Collection<String> unused = new TreeSet<>();
        System.out.println("DeclaredType,SubType,PropertyType,Name,Java Declaration,Code To Remove");
        for (NodeClass nodeClass : finder.getNodes().values()) {
            if (!nodeClass.fullyUsed()) {
                if (nodeClass.isReferenced && nodeClass.isConcrete()) {
                    String name = nodeClass.getJavaName();
                    for (String field : nodeClass.fields) {
                        unused.add(name + "." + field);
                        // technically incorrect, not checking for public field inheritance here
                        System.out.println(nodeClass.name + "," + name + ",FIELD," + field);
                    }
                    for (NodeClass.Method method : nodeClass.methods) {
                        unused.add(method.getJavaString(name) + " -- " + method.descriptor);
                        System.out.println(method.className + "," + name + ",METHOD," + method.name + ",\"" + method.getJavaString(null) + "\",\""
                                + ".removeMethod(\"\"" + method.name + "\"\", \"\"" + method.descriptor + ")\"");
                    }
                }
            } else {
                fullyUsed2++;
            }
        }
        System.out.println("Unused: " + unused.size());
        assertThat(unused, empty());
    }

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

    public static class PropertyChecker extends ClassVisitor{

        private Map<String, NodeClass> nodes;

        public PropertyChecker(Map<String, NodeClass> nodes) {
            super(Opcodes.ASM5);
            this.nodes = nodes;
        }

        private void markNodesAsVisited(String descriptor, int maxCount) {
            Collection<String> referencedTypes = getParameterAndReturnTypes(descriptor);
            assertTrue("Expected Less than " + maxCount + ", but got: " + referencedTypes.size(),
                    referencedTypes.size() < maxCount);
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
            // TODO
            return new ArrayList<>();
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
                    System.out.println("FIELD: " + owner + "." + name + " " + desc);
                }
            }

            @Override
            public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
                markNodesAsVisited(desc, 1);
            }

            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
                if (nodes.containsKey(owner)) {
                    nodes.get(owner).usedMethod(name, desc);
                }
            }

            @Override
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
        public Set<String> fields;
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

        public String getBaseClassName() {
            return baseClassName;
        }

        public NodeClass getBaseClass() {
            return baseClass;
        }

        public void addField(int access, String fieldName) {
            if ((access & Opcodes.ACC_PUBLIC) > 0) {
                if ((access & Opcodes.ACC_STATIC) == 0) {
                    fields.add(fieldName);
                    System.out.println("WARNING " + getJavaName() + " has a public field: " + fieldName);
                }
            }
        }

        public Method addMethod(int access, String name, String descriptor) {
            if ((access & Opcodes.ACC_PUBLIC) > 0) {
                if ((access & Opcodes.ACC_STATIC) == 0) {
                    if (name.startsWith("get")) {
                        Method method = new Method(this.name, name, descriptor);
                        methods.add(method);
                        return method;
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
                    fields.addAll(baseClass.fields);
                    methods.addAll(baseClass.methods);
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(getJavaName());
            stringBuilder.append(": ");
            for (String field : fields) {
                stringBuilder.append(field);
                stringBuilder.append(", ");
            }
            for (Method method: methods) {
                stringBuilder.append(method);
                stringBuilder.append(", ");
            }
            return stringBuilder.toString();
        }

        public void usedMethod(String name, String desc) {
            if (name.startsWith("get")) {
                removeMethod(name, desc);
            }
        }

        public void usedField(String name) {
            fields.remove(name);
        }

        public boolean fullyUsed() {
            return methods.size() == 0 && fields.size() == 0;
        }

        public void reference() {
            isReferenced = true;
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

        public static class Method {

            private final String descriptor;
            private final String className;
            private String name;

            public Method(String className, String name, String descriptor) {
                this.className = className;
                this.name = name;
                this.descriptor = descriptor;
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

            private String typeToString(Type type) {
                String className = type.getClassName();
                if (type.getSort() == Type.OBJECT) {
                    className = className.substring(className.lastIndexOf('.')+1);
                }
                return className;
            }
        }
    }


}


