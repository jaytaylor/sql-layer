from testbase import *

run_plan("Scan the customer name index for 'ori' and then find the row",
         IndexedTableScan(IndexScan(customer_name_index, ['ori']), ['hkey'], coi))
