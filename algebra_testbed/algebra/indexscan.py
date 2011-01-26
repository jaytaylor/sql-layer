import scan

Scan = scan.Scan

class IndexScan(Scan):

    def __init__(self, index, key = None):
        Scan.__init__(self, index.cursor(key, key))
