import pandas
import openpyxl
import os
import time
from extract.excel_extract import work_proc_with_thread
from extract.excel_extract import work_only_proc
from extract.excel_extract import work_pool

start = time.time()


def excel_to_csv(work_type='proc_with_thread'):

    if work_type == 'proc_with_thread':
        work_proc_with_thread()
    elif work_type == 'pool':
        work_pool()
    elif work_type == 'only_proc':
        work_only_proc()

    print('소요 시간 {}sec'.format(round((time.time() - start), 2)))

