/**
 * 
 */
package com.akiban.cserver.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.vstore.ColumnArrayGenerator;
import com.akiban.vstore.HKeyColumnArrayGenerator;
import com.akiban.vstore.IColumnDescriptor;
import com.akiban.vstore.VMeta;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

/**
 * @author percent
 * 
 */
public class GroupGenerator {

    public GroupGenerator(String prefix, AkibaInformationSchema ais, RowDefCache cache) {
        randomProjection = false;
        this.ais = ais;
        this.prefix = prefix;
        this.rowDefCache = cache;
        rowData = new ArrayList<RowData>();
        columnDes = new ArrayList<IColumnDescriptor>();
        hkeyDes = new ArrayList<IColumnDescriptor>();
        rowsPerTable = new TreeMap<Integer, Integer>();
        encodedColumnMap = new TreeMap<Integer, ArrayList<ArrayList<byte[]>>>(); 
        columnMap = new TreeMap<Integer, ArrayList<ColumnArrayGenerator>>();
        hkeyMap = new TreeMap<Integer, HKeyColumnArrayGenerator>();
        projections = new TreeMap<Integer, BitSet>();
        bitMaps = new TreeMap<Integer, byte[]>();
    }

    public void generateGroup(RowDef groupRowDef) throws Exception {
        Table table = ais.getTable(groupRowDef.getSchemaName(), groupRowDef.getTableName());
        assert table.isGroupTable();
        UserTable utable = ((GroupTable) table).getRoot();
        setup(utable, 2);
        setupGroupBitMap(table);

        Key key = new Key((Persistit) null);
        key.clear();
        generateRows(utable, key, 0, 2);

        writeColumns(utable);
        generateDescriptors(utable);
        meta = new VMeta(hkeyDes, columnDes);
    }

    public ArrayList<RowData> getRows() { 
         return rowData;
    }
    
    public VMeta getMeta() {
        return meta;
    }
    
    public byte[] getGroupBitMap() {
        return groupBitMap;
    }
    
    public int getGroupSize() {
        return groupSize;
    }
    
    private void setupGroupBitMap(Table table) {
        int mapSize = table.getColumns().size()/8;
        if (table.getColumns().size() % 8 != 0) {
            mapSize++;
        }
        groupBitMap = new byte[mapSize];
        for(int i=0 ; i < groupBitMap.length; i++) {
            for(int j = 0 ; j < 8; j++) {
                groupBitMap[i] |= 1 << j % 8;
            }
        }
        // j < table.getColumns().size(); j++) {
        Iterator<Integer> i = projections.keySet().iterator();
        
        int offset = 0;
        int position = 0;
        while(i.hasNext()) {
            Integer key = i.next();
            
            BitSet projection = projections.get(key);
            int fields = rowDefCache.getRowDef(key).getFieldCount();
            //System.out.println("Group gen: table = " +rowDefCache.getRowDef(key).getTableName()+" fields "+fields+ "bitmap.length ="+groupBitMap.length);
            for(position = 0; position < fields; position++) {
                if(projection.get(position)) {
                    groupBitMap[offset] |= 1 << position % 8;
                } else {
//                    System.out.println("group gen: position "+position);
                    assert false;
                }
                position++;
                if (position % 8 == 0) {
                    offset++;
                    assert offset < groupBitMap.length;
                }
            }
        }
        for(; position % 8 != 0; position++) {
            groupBitMap[offset] |= 1 << position % 8;
        }
    }
    
    private void generateProjection(UserTable table, int fieldCount) {
        BitSet projection = new BitSet(fieldCount);
        int mapSize = fieldCount / 8;
        if (fieldCount % 8 != 0) {
            mapSize++;
        }

        byte[] bitMap = new byte[mapSize];
        bitMap[0] |= 1;
        projection.set(0);

        for (int j = 1, offset = 0; j < fieldCount; j++) {
            if (randomProjection) {
                projection.set(j, rand.nextBoolean());
            } else {
                projection.set(j, true);
            }
            if (projection.get(j)) {
                bitMap[offset] |= 1 << j % 8;
            }
            if ((j + 1) % 8 == 0) {
                offset++;
            }
        }
        projections.put(table.getTableId(), projection);
        bitMaps.put(table.getTableId(), bitMap);
    }

    private void generateDescriptors(UserTable table) throws Exception {

        RowDef rowDef = rowDefCache.getRowDef(table.getTableId());
        FieldDef[] fields = rowDef.getFieldDefs();
        BitSet projection = projections.get(table.getTableId());
        // System.out.println("prefix = "+prefix+table.getName().getSchemaName()
        // +table.getName().getTableName()+table.getTableId());
        hkeyDes.add(IColumnDescriptor.create(prefix, 
                table.getName().getSchemaName(), table.getName().getTableName(), table.getTableId(), 
                hkeyMap.get(table.getTableId()).getKeys().size()));
                
        for (int i = 0; i < fields.length; i++) {
            if (projection.get(i)) {
                assert fields[i] != null;
                columnDes.add(IColumnDescriptor.create(prefix, 
                        table.getName().getSchemaName(), 
                        table.getName().getTableName(), fields[i].getName(),
                        rowDef.getRowDefId(), i, fields[i].getMaxStorageSize(), 
                        rowsPerTable.get(table.getTableId())));
            } else {
                assert false;
            }
        }
    
        Iterator<Join> children = table.getChildJoins().iterator();
        while (children.hasNext()) {
            Join k = children.next();
            UserTable child = k.getChild();
            generateDescriptors(child);
        }
    }
    
    private void setup(UserTable table, int rows) throws Exception {
        RowDef rowDef = rowDefCache.getRowDef(table.getTableId());

        File directory = new File(prefix);
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new Exception();
            }
        }

        String schemaName = rowDef.getSchemaName();
        String tableName = rowDef.getTableName();
        String pathPrefix = prefix + schemaName + tableName;

        FieldDef[] fields = rowDef.getFieldDefs();
        assert fields.length == rowDef.getFieldCount();
        int rowSize = 0;

        generateProjection(table, fields.length);
        BitSet projection = projections.get(table.getTableId());
        ArrayList<ColumnArrayGenerator> columns = new ArrayList<ColumnArrayGenerator>();
        ArrayList<ArrayList<byte[]>> encodedColumns = new ArrayList<ArrayList<byte[]>>();
        for (int i = 0; i < fields.length; i++) {
            assert fields[i].isFixedSize() == true;
            if (projection.get(i)) {
                rowSize += fields[i].getMaxStorageSize();
                columns.add(new ColumnArrayGenerator(pathPrefix
                        + fields[i].getName(), 1337 + i, fields[i]
                        .getMaxStorageSize(), rows));
                encodedColumns.add(new ArrayList<byte[]>());
            } else {
                assert false;
            }
        }

        HKeyColumnArrayGenerator hkeyGen = new HKeyColumnArrayGenerator(pathPrefix);

        hkeyMap.put(table.getTableId(), hkeyGen);
        rowsPerTable.put(table.getTableId(), 0);
//        System.out.println("group gen: "+table.getName().getTableName()+ ", rows = "+rows);
        columnMap.put(table.getTableId(), columns);
        encodedColumnMap.put(table.getTableId(), encodedColumns);
        
        Iterator<Join> children = table.getChildJoins().iterator();
        while (children.hasNext()) {
            Join j = children.next();
            UserTable child = j.getChild();
            setup(child, rows*2);
        }
    }

    private void generateARow(UserTable table, Key keyPrefix) throws Exception {

        BitSet projection = projections.get(table.getTableId());
        ArrayList<ColumnArrayGenerator> columns = columnMap.get(table.getTableId());
        ArrayList<ArrayList<byte[]>> encodedColumns = encodedColumnMap.get(table.getTableId());
        
        int rows = rowsPerTable.get(table.getTableId());
        rows++;
        rowsPerTable.put(table.getTableId(), rows);

        HKeyColumnArrayGenerator hkeys = hkeyMap.get(table.getTableId());
        hkeys.append(new KeyState(keyPrefix), 1);

        // God, why can't this be easier?
        RowDef rowDef = rowDefCache.getRowDef(table.getTableId());
        FieldDef[] fields = rowDef.getFieldDefs();
        int rowSize = 0;
        for (int i = 0; i < fields.length; i++) {
            assert fields[i].isFixedSize() == true;
            if (projection.get(i)) {
                rowSize += fields[i].getMaxStorageSize();
            } else {
                assert false;
            }
        }
        
        int totalRowSize = rowSize+RowData.MINIMUM_RECORD_LENGTH
        +((rowDef.getFieldCount() % 8) == 0 ? rowDef.getFieldCount() / 8
                : rowDef.getFieldCount() / 8 + 1);
        Object[] aRow = new Object[rowDef.getFieldCount()];
        RowData aRowData = new RowData(
                new byte[totalRowSize]);
        groupSize += totalRowSize;

        for (int j = 0, k = 0; j < rowDef.getFieldCount(); j++) {
            if (projection.get(j)) {
                byte[] b = columns.get(k).generateMemoryFile(1).get(0);
                assert b.length == 4;
                k++;
                int rawFieldInt = ((int) (b[0] & 0xff) << 24)
                        | ((int) (b[1] & 0xff) << 16)
                        | ((int) (b[2] & 0xff) << 8) | (int) b[3] & 0xff;
                aRow[j] = rawFieldInt;
            } else {
                aRow[j] = null;
            }
        }

        aRowData.createRow(rowDef, aRow);
        rowData.add(aRowData);

        for (int j = 0, k = 0; j < rowDef.getFieldCount(); j++) {
            if (projection.get(j)) {
                long offset_width = rowDef.fieldLocation(aRowData, j);
                int offset = (int) offset_width;
                int width = (int) (offset_width >>> 32);
                byte[] bytes = aRowData.getBytes();
                byte[] field = new byte[width];
                for (int l = 0; l < width; l++) {
                    field[l] = bytes[offset + l];
                }
                encodedColumns.get(k).add(field);
                k++;
            }
        }
    }

    private void generateRows(UserTable table, Key keyPrefix, int offset, int rows) throws Exception {
        
        for (int i = offset; i < offset+rows; i++) {
            Key newKey = new Key(keyPrefix);
            newKey.append(i);
            //System.out.println("creating row = "+table.getName().getTableName()+" key = "+newKey);
            generateARow(table, newKey);
            Iterator<Join> children = table.getChildJoins().iterator();
            
            int newOffset=0;
            while (children.hasNext()) {
                Join j = children.next();
                UserTable child = j.getChild();
                generateRows(child, newKey, newOffset, rows);
                newOffset += rows;
            }
        }
    }
    
    private void writeColumns(UserTable table) throws Exception {
        BitSet projection = projections.get(table.getTableId());
        ArrayList<ColumnArrayGenerator> columns = columnMap.get(table.getTableId());
        ArrayList<ArrayList<byte[]>> encodedColumns = encodedColumnMap.get(table.getTableId());
        RowDef rowDef = rowDefCache.getRowDef(table.getTableId());
        FieldDef[] fields = rowDef.getFieldDefs();
        
        for (int i = 0, j = 0; i < fields.length; i++) {
            if (projection.get(i)) {
                columns.get(j).writeEncodedColumn(encodedColumns.get(j));
                j++;
            }
        }
        Iterator<Join> children = table.getChildJoins().iterator();
        while (children.hasNext()) {
            Join j = children.next();
            UserTable child = j.getChild();
            writeColumns(child);
        }
    }
    
    private final String prefix;
    //private final int rows = 2;
    private final Random rand = new Random(31337);
    
    private boolean randomProjection;
    private RowDefCache rowDefCache;
    private AkibaInformationSchema ais;
    
    private ArrayList<RowData> rowData;
    private VMeta meta;
    private byte[] groupBitMap;
    private int groupSize;
    private ArrayList<IColumnDescriptor> columnDes;
    private ArrayList<IColumnDescriptor> hkeyDes;
    private TreeMap<Integer, Integer> rowsPerTable;
    private TreeMap<Integer, ArrayList<ArrayList<byte[]>>> encodedColumnMap;
    private TreeMap<Integer, ArrayList<ColumnArrayGenerator>> columnMap;
    private TreeMap<Integer, HKeyColumnArrayGenerator> hkeyMap;
    private TreeMap<Integer, BitSet> projections;
    private TreeMap<Integer, byte[]> bitMaps;

}
