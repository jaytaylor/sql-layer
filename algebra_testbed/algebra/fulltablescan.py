import scan

Scan = scan.Scan

class FullTableScan(Scan):

    def __init__(self, table):
        Scan.__init__(self, table.cursor())
