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

package com.foundationdb.protobuf;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class ProtobufDecompiler
{
    protected static final Map<Label,String> LABELS = new HashMap<>();
    protected static final Map<Type,String> TYPES = new HashMap<>();
    static {
        LABELS.put(Label.LABEL_OPTIONAL, "optional");
        LABELS.put(Label.LABEL_REPEATED, "repeated");
        LABELS.put(Label.LABEL_REQUIRED, "required");

        // Simple types only.
        TYPES.put(Type.TYPE_BOOL, "bool");
        TYPES.put(Type.TYPE_BYTES, "bytes");
        TYPES.put(Type.TYPE_DOUBLE, "double");
        TYPES.put(Type.TYPE_FIXED32, "fixed32");
        TYPES.put(Type.TYPE_FIXED64, "fixed64");
        TYPES.put(Type.TYPE_FLOAT, "float");
        TYPES.put(Type.TYPE_INT32, "int32");
        TYPES.put(Type.TYPE_INT64, "int64");
        TYPES.put(Type.TYPE_SFIXED32, "sfixed32");
        TYPES.put(Type.TYPE_SFIXED64, "sfixed64");
        TYPES.put(Type.TYPE_SINT32, "sint32");
        TYPES.put(Type.TYPE_SINT64, "sint64");
        TYPES.put(Type.TYPE_STRING, "string");
        TYPES.put(Type.TYPE_UINT32, "uint32");
        TYPES.put(Type.TYPE_UINT64, "uint64");
    }
    protected final Appendable output;
    protected String newline = System.lineSeparator();
    protected int indent = 0;
    protected int tabWidth = 4;
    protected boolean indentWithTabs = false;
    protected String absolutePackage = null;

    public ProtobufDecompiler(Appendable output) {
        this.output = output;
    }

    public ProtobufDecompiler(OutputStream output) {
        this.output = new BufferedWriter(new OutputStreamWriter(output));
    }

    public static void main(String[] args) throws IOException {
        ProtobufDecompiler decompiler = new ProtobufDecompiler((Appendable)System.out);
        FileDescriptorSet set;
        try (FileInputStream istr = new FileInputStream(args[0])) {
            set = FileDescriptorSet.parseFrom(istr);
        }
        decompiler.decompile(set);
    }

    public void decompile(FileDescriptorSet setDescriptor) throws IOException {
        for (FileDescriptorProto fileDescriptor : setDescriptor.getFileList()) {
            newline();
            format("===== %s =====", fileDescriptor.getName());
            newline();
            decompile(fileDescriptor);
        }
    }

    public void decompile(FileDescriptorProto fileDescriptor) throws IOException {
        if (fileDescriptor.hasPackage()) {
            indentedFormat("package %s;", fileDescriptor.getPackage());
            absolutePackage = "." + fileDescriptor.getPackage() + ".";
        }
        for (String dependency : fileDescriptor.getDependencyList()) {
            indentedFormat("import \"%s\";", dependency);
        }
        if (fileDescriptor.hasOptions()) {
            decompileOptions(fileDescriptor.getOptions());
        }
        decompileMembers(fileDescriptor.getEnumTypeList(),
                         fileDescriptor.getMessageTypeList(),
                         Collections.<FieldDescriptorProto>emptyList(),
                         Collections.<DescriptorProto.ExtensionRange>emptyList(),
                         fileDescriptor.getExtensionList());
        for (ServiceDescriptorProto serviceDescriptor : fileDescriptor.getServiceList()) {
            decompile(serviceDescriptor);
        }
        newline();
        flush();
    }

    protected void decompile(EnumDescriptorProto enumDescriptor) throws IOException {
        indentedFormat("enum %s {", enumDescriptor.getName());
        indent++;
        if (enumDescriptor.hasOptions()) {
            decompileOptions(enumDescriptor.getOptions());
        }
        for (EnumValueDescriptorProto value : enumDescriptor.getValueList()) {
            indentedFormat("%s = %d%s;", value.getName(), value.getNumber(),
                           value.hasOptions() ? bracketedOptions(value.getOptions()) : "");
        }
        indent--;
        indentedFormat("}");
    }

    protected void decompile(DescriptorProto messageDescriptor) throws IOException {
        indentedFormat("message %s {", messageDescriptor.getName());
        decompileMessageBody(messageDescriptor);
    }

    protected void decompile(ServiceDescriptorProto serviceDescriptor) throws IOException {
        indentedFormat("service %s {", serviceDescriptor.getName());
        indent++;
        if (serviceDescriptor.hasOptions()) {
            decompileOptions(serviceDescriptor.getOptions());
        }
        for (MethodDescriptorProto methodDescriptor : serviceDescriptor.getMethodList()) {
            indentedFormat("rpc %s (%s) returns (%s)",
                           methodDescriptor.getName(), methodDescriptor.getInputType(), methodDescriptor.getOutputType());
            if (methodDescriptor.hasOptions()) {
                write("{ ");
                indent++;
                decompileOptions(methodDescriptor.getOptions());
                indent--;
                indentedFormat("}");
            }
            else {
                write(";");
            }
        }
        indent--;
        indentedFormat("}");
    }

    protected void decompileMessageBody(DescriptorProto messageDescriptor) throws IOException {
        indent++;
        if (messageDescriptor.hasOptions()) {
            decompileOptions(messageDescriptor.getOptions());
        }
        decompileMembers(messageDescriptor.getEnumTypeList(),
                         messageDescriptor.getNestedTypeList(),
                         messageDescriptor.getFieldList(),
                         messageDescriptor.getExtensionRangeList(),
                         messageDescriptor.getExtensionList());
        indent--;
        indentedFormat("}");
    }

    protected void decompileMembers(List<EnumDescriptorProto> enumDescriptors,
                                    List<DescriptorProto> messageDescriptors,
                                    List<FieldDescriptorProto> fieldDescriptors,
                                    List<DescriptorProto.ExtensionRange> extensionRanges,
                                    List<FieldDescriptorProto> extensions) 
            throws IOException {
        for (EnumDescriptorProto enumDescriptor : enumDescriptors) {
            decompile(enumDescriptor);
        }
        Map<String,DescriptorProto> groups = new HashMap<>();
        findGroups(fieldDescriptors, groups);
        findGroups(extensions, groups);
        for (DescriptorProto nestedDescriptor : messageDescriptors) {
            if (groups.containsKey(nestedDescriptor.getName())) {
                groups.put(nestedDescriptor.getName(), nestedDescriptor);
            }
            else {
                decompile(nestedDescriptor);
            }
        }
        decompileFields(fieldDescriptors, groups);
        for (DescriptorProto.ExtensionRange extensionRange : extensionRanges) {
            indentedFormat("extensions %s to %s;", extensionRange.getStart(), 
                           (extensionRange.getEnd() == 0x20000000) ? "max" : extensionRange.getEnd());
        }
        if (!extensions.isEmpty()) {
            Map<String,List<FieldDescriptorProto>> extensionsByExtendee = new TreeMap<>();
            for (FieldDescriptorProto extension : extensions) {
                List<FieldDescriptorProto> entry = extensionsByExtendee.get(extension.getExtendee());
                if (entry == null) {
                    entry = new ArrayList<>();
                    extensionsByExtendee.put(extension.getExtendee(), entry);
                }
                entry.add(extension);
            }
            for (Map.Entry<String,List<FieldDescriptorProto>> entry : extensionsByExtendee.entrySet()) {
                indentedFormat("extend %s {", entry.getKey());
                indent++;
                decompileFields(entry.getValue(), groups);
                indent--;
                indentedFormat("}");
            }
        }
    }

    protected void findGroups(List<FieldDescriptorProto> fieldDescriptors,
                              Map<String,DescriptorProto> groups) {
        for (FieldDescriptorProto fieldDescriptor : fieldDescriptors) {
            if (fieldDescriptor.getType() == Type.TYPE_GROUP) {
                groups.put(fieldDescriptor.getTypeName(), null);
            }
        }
    }

    protected void decompileFields(List<FieldDescriptorProto> fieldDescriptors,
                                   Map<String,DescriptorProto> groups)
            throws IOException {
        for (FieldDescriptorProto fieldDescriptor : fieldDescriptors) {
            String label = LABELS.get(fieldDescriptor.getLabel());
            String type = TYPES.get(fieldDescriptor.getType());
            String name = fieldDescriptor.getName();
            if (fieldDescriptor.hasTypeName()) {
                type = fieldDescriptor.getTypeName();
                if ((absolutePackage != null) && type.startsWith(absolutePackage)) {
                    type = type.substring(absolutePackage.length());
                }
            }
            DescriptorProto groupDescriptor = null;
            if (fieldDescriptor.getType() == Type.TYPE_GROUP) {
                groupDescriptor = groups.get(type);
                if (groupDescriptor != null) {
                    name = type;
                    type = "group";
                }
            }
            indentedFormat("%s %s %s = %d",
                           label, type, name, fieldDescriptor.getNumber());
            if (fieldDescriptor.hasOptions() || fieldDescriptor.hasDefaultValue()) {
                write(defaultAndOptions(fieldDescriptor.hasOptions() ? fieldDescriptor.getOptions() : null,
                                        fieldDescriptor.hasDefaultValue() ? fieldDescriptor.getDefaultValue() : null));
            }
            if (groupDescriptor == null) {
                write(";");
            }
            else {
                decompileMessageBody(groupDescriptor);
            }
        }
    }

    protected void decompileOptions(MessageOrBuilder options) throws IOException {
        for (Map.Entry<FieldDescriptor,Object> entry : options.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            Object value = entry.getValue();
            String fieldName = field.getName();
            if (field.isExtension()) {
                fieldName = "(" + fieldName + ")";
            }
            if (field.getType() == FieldDescriptor.Type.MESSAGE) {
                for (Map.Entry<FieldDescriptor,Object> subentry : ((MessageOrBuilder)value).getAllFields().entrySet()) {
                    FieldDescriptor subfield = subentry.getKey();
                    Object subvalue = subentry.getValue();
                    indentedFormat("option %s.%s = %s;", fieldName, subfield.getName(), literal(subvalue, subfield.getType()));
                }
            }
            else {
                indentedFormat("option %s = %s;", fieldName, literal(value, field.getType()));
            }
        }
    }

    protected String bracketedOptions(MessageOrBuilder options) {
        return defaultAndOptions(options, null);
    }

    protected String defaultAndOptions(MessageOrBuilder options, String defaultValue) {
        StringBuilder str = new StringBuilder();
        boolean first = true;
        if (defaultValue != null) {
            str.append(" [default = ");
            str.append(defaultValue); // TODO: quote
            first = false;
        }
        if (options != null) {
            for (Map.Entry<FieldDescriptor,Object> entry : options.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                Object value = entry.getValue();
                String fieldName = field.getName();
                if (field.isExtension()) {
                    fieldName = "(" + fieldName + ")";
                }
                if (field.getType() == FieldDescriptor.Type.MESSAGE) {
                    for (Map.Entry<FieldDescriptor,Object> subentry : ((MessageOrBuilder)value).getAllFields().entrySet()) {
                        FieldDescriptor subfield = subentry.getKey();
                        Object subvalue = subentry.getValue();
                        if (first) {
                            str.append(" [");
                            first = false;
                        }
                        else {
                            str.append(", ");
                        }
                        str.append(fieldName).append(".").append(subfield.getName()).append(" = ").append(literal(subvalue, subfield.getType()));
                    }
                }
                else {
                    if (first) {
                        str.append(" [");
                        first = false;
                    }
                    else {
                        str.append(", ");
                    }
                    str.append(fieldName).append(" = ").append(literal(value, field.getType()));
                }
            }
        }
        if (!first) {
            str.append("]");
        }
        return str.toString();
    }

    protected String literal(Object value, FieldDescriptor.Type type) {
        switch (type) {
        case STRING:
            return quotedString((String)value);
        default:
            return value.toString();
        }
    }

    protected String quotedString(String str) {
        return "\"" + str.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    public String getNewline() {
        return newline;
    }
    public void setNewline(String newline) {
        this.newline = newline;
    }

    public int getTabWidth() {
        return tabWidth;
    }
    public void setTabWidth(int tabWidth) {
        this.tabWidth = tabWidth;
    }
    
    public boolean isIndentWithTabs() {
        return indentWithTabs;
    }
    public void setIndentWithTabs(boolean indentWithTabs) {
        this.indentWithTabs = indentWithTabs;
    }

    protected void indentedFormat(String fmt, Object... args) throws IOException {
        indentedLine();
        format(fmt, args);
    }

    protected void format(String fmt, Object... args) throws IOException {
        write(String.format(fmt, args));
    }

    protected void write(String str) throws IOException {
        output.append(str);
    }

    protected void newline() throws IOException {
        write(newline);
    }

    protected void indentedLine() throws IOException {
        newline();
        for (int i = 0; i < indent; i++) {
            if (indentWithTabs) {
                write("\t");
            }
            else {
                for (int j = 0; j < tabWidth; j++) {
                    write(" ");
                }
            }
        }
    }

    protected void flush() throws IOException {
        if (output instanceof Flushable) {
            ((Flushable)output).flush();
        }
    }
}
