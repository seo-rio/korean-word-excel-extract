import os
import math
import pandas
import openpyxl
# from excel.extract.excel_to_csv import extract
from multiprocessing import Process
from multiprocessing import Queue
from threading import Thread
from multiprocessing import Pool


# 사용가능한 프로세스 개수
use_cpu = int(os.cpu_count() / 2)
path = './xlsx_file'
file_list = os.listdir(path)
file_count = len(file_list)

thread_result_arr = []
proc_result_arr = []


# 코어당 스레드 균등 분배
def thread_equal_distb(f_cnt, c_cnt):
    one_core_thread = []

    # 같은 비율일 경우 각 코어당 스레드 분배 개수
    same_rate_cnt = f_cnt / c_cnt
    # 균등하게 나눌 경우 코어당 스레드 분배가 더 많은 개수
    lot_c_cnt = f_cnt % c_cnt
    # 균등하게 나눌 경우 코어당 스레드 분배가 더 적은 개수
    ltl_c_cnt = c_cnt - lot_c_cnt

    for i in range(0, lot_c_cnt):
        one_core_thread.append(math.ceil(same_rate_cnt))

    for i in range(0, ltl_c_cnt):
        one_core_thread.append(math.floor(same_rate_cnt))

    return one_core_thread


queue = Queue()



def work_thread(file_path_arr):

    arr_len = len(file_path_arr)

    result_arr = []

    for i in range(0, arr_len):
        th = Thread(target=extract, args=(file_path_arr[i],))
        th.start()
        th.join()

    for obj in thread_result_arr:
        # print(obj)
        result_arr.append(obj)

    print(len(result_arr))
    queue.put(result_arr)
    print('END Thread')
    # my_data = my_queue.get())
    # print(proc_result_arr)


# 프로세스 + 스레드
def work_proc_with_thread():

    global queue

    procs = []

    df = None
    # 파일 목록 배열에서 각 코어당 배정될 스레드 수만큼 파일을 나눠서 분배
    arr_st_cnt = 0

    for thread_count in thread_equal_distb(file_count, use_cpu):
        file_path_arr = []

        for i in range(arr_st_cnt, arr_st_cnt + thread_count):
            file_path_arr.append(path + '/' + file_list[i])
            # work_thread(file_list[i])

        proc = Process(target=work_thread, args=(file_path_arr, ))
        proc.start()
        procs.append(proc)

        # print(df)
        arr_st_cnt += thread_count

    for proc in procs:
        proc.join()

        # 여기서 왜 프로세스가 종료가 안되는지 찾아내야함
        print('END Proc Join')

    print('END Proc')
    queue.put('exit')

    test_arr = []
    while True:
        tmp = queue.get()
        if tmp == 'exit':
            break
        else:
            test_arr.append(tmp)

    for obj in test_arr:
        print(pandas.DataFrame(obj))

    # https://velog.io/@soojung61/Python-Process 링크확인..

# Pool을 이용한 작업
def work_pool():
    pool = Pool(processes=use_cpu)

    file_arr = []
    for i in range(0, file_count):
        file_arr.append(path + '/' + file_list[i])

    outputs = pool.map(extract, file_arr)
    print(outputs)


# 순수 프로세스만을 이용한 작업
def work_only_proc():

    procs = []

    for i in range(0, file_count):
        proc = Process(target=extract, args=(path + '/' + file_list[i],))
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()


def convert_text(text):
    return_text = text.replace('-', '')
    count = return_text.find('(')

    if count > -1:
        return_text = return_text[0:count]

    return return_text


def extract(file_path):

    global thread_result_arr
    # lock.acquire()
    # try:
    #     csv_name += (csv_name +1)
    # finally:
    #     lock.release()

    print('프로세스 PID: {}, 파일 [{}] 작업 시작'.format(os.getpid(), file_path))
    wb = openpyxl.load_workbook(file_path)

    ws = wb.active

    df_obj = {
        'word': [],
        'word_type': [],
        'desc': []
    }

    for row in ws.rows:
        text = row[0].value
        word_type = row[10].value.replace('「', '').replace('」', '').replace('\n', '')
        desc = row[15].value

        # print('A : ', word_type in '명사')
        # print('B : ', row[10].value)

        if word_type in '명사':
            df_obj['word'].append(convert_text(text))
            df_obj['word_type'].append(word_type)
            df_obj['desc'].append(desc)

            # print('단어 : {}, 품사: {} \n뜻: {}'.format(convert_text(text), word_type, desc))

    thread_result_arr.append(df_obj)
    # df = pandas.DataFrame(df_obj)

    save_name = file_path[file_path.rfind('/') + 1:file_path.rfind('.')] + '.csv'
    # df.to_csv(save_name, encoding='utf-8-sig')
    print('프로세스 PID: {}, 파일 [{}] 작업 종료'.format(os.getpid(), file_path))
    # return df
    return df_obj



