import openpyxl
import os
import pandas
import math
from multiprocessing import Process
from multiprocessing import Pool
from threading import Thread
import time
import re
import sys


# 사용가능한 프로세스 개수
use_cpu = int(os.cpu_count())

path = './xlsx_file'
file_list = os.listdir(path)
file_count = len(file_list)


def convert_text(text):
    return_text = text.replace('-', '')
    count = return_text.find('(')

    if count > -1:
        return_text = return_text[0:count]

    return return_text


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


def work_thread(file_path_arr):

    arr_len = len(file_path_arr)

    for i in range(0, arr_len):
        th = Thread(target=extract, args=(file_path_arr[i],))
        th.start()
        th.join()


# Pool을 이용한 작업
def work_pool():
    pool = Pool(processes=use_cpu)

    file_arr = []
    for i in range(0, file_count):
        file_arr.append(path + '/' + file_list[i])

    outputs = pool.map(extract, file_arr)
    print(outputs)


# 순수 프로세스만을 이용한 작업
def only_work_proc():

    procs = []

    for i in range(0, file_count):
        proc = Process(target=extract, args=(path + '/' + file_list[i],))
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()


# 프로세스 + 스레드
def work_proc_with_thread():

    procs = []
    # 파일 목록 배열에서 각 코어당 배정될 스레드 수만큼 파일을 나눠서 분배
    arr_st_cnt = 0

    for thread_count in thread_equal_distb(file_count, use_cpu):
        file_path_arr = []

        for i in range(arr_st_cnt, arr_st_cnt + thread_count):
            file_path_arr.append(path + '/' + file_list[i])
            # work_thread(file_list[i])
        proc = Process(target=work_thread, args=(file_path_arr,))
        proc.start()
        procs.append(proc)

        arr_st_cnt += thread_count

    for proc in procs:
        proc.join()


def extract(file_path):
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

    df = pandas.DataFrame(df_obj)

    print(df)
    print('프로세스 PID: {}, 파일 [{}] 작업 종료'.format(os.getpid(), file_path))
    # df.to_csv('1.csv', encoding='utf-8-sig')


if __name__ == '__main__':

    loop_count = 3
    total_time = 0

    for i in range(0, loop_count):
        start = time.time()
        print('총 [{}]개 작업 중, [{}] 번째 작업 시작'.format(loop_count, i + 1))
        # only_work_proc()  # 48.72 sec cpu 50% 정도
        work_proc_with_thread()  # 43.66 거의 cpu 90 % 이상
        # work_pool() 44.99 거의 90% 이상
        print('소요 시간 {}초'.format(round((time.time() - start), 2)))
        total_time += round((time.time() - start), 2)

    print("평균 소요 시간 :", total_time / loop_count)


