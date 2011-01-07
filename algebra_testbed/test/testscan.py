from testbase import *

run_plan("index scan and join",
         Join
         (IndexScan(customer_name_index),
          ['hkey'],
          coi,
          Tc,
          KEEP_RIGHT))
