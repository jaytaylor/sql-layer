import schema.rowtype
import row

RowType = schema.rowtype.RowType
Row = row.Row

class Map(object):

    def __init__(self):
        self._extent = []

    def __iter__(self):
        return self._extent.__iter__()

    def add(self, row):
        self._extent.append(row)

    def close(self):
        self._extent.sort(key = lambda row: row.key)

    def add_index(self, rowtype, key_fields):
        return Index(self, rowtype, key_fields)

    def lookup(self, key):
        values = []
        for row in self._extent:
            if row.key == key:
                values.append(row)
        return values

class Index(Map):

    def __init__(self, map, table_rowtype, index_rowtype):
        Map.__init__(self)
        for row in map:
            if row.rowtype is table_rowtype:
                index_value = {}
                for field in index_rowtype.value:
                    index_value[field] = row[field]
                self.add(Row(index_rowtype, index_value))
        self.close()
