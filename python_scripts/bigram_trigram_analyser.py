import os
import json
import re
from collections import defaultdict
import time
import multiprocessing
import copy
import asyncio
import threading
import utils
import cProfile
import pstats


ABS_PATH = os.path.abspath(__file__)
PARENT_PATH = os.path.abspath(os.path.join(os.path.dirname(ABS_PATH), os.pardir))
OUTPUT_PATH = f"{PARENT_PATH}/output/python_version"

def analyse_ebook_multiprocess(ebook_path_list:str, analysis_dict:dict, result_queue:multiprocessing.Queue):
    for ebook_path in ebook_path_list:
        utils.analyse_ebook(ebook_path, analysis_dict)

    result_queue.put(analysis_dict)

async def analyse_ebook_asyncio(ebook_path_list:str, analysis_dict:dict):
    for ebook_path in ebook_path_list:
        utils.analyse_ebook(ebook_path, analysis_dict)  

def analyse_ebook_multithreads(ebook_path_list:str, analysis_dict:dict):
    for ebook_path in ebook_path_list:
        utils.analyse_ebook(ebook_path, analysis_dict)

def load_and_analysis_seq(ebooks_path:str, n_ebooks:int, analysis_types:dict, to_json:bool = False):

    # Initialization of dictionaries based on analysis
    analysis_dict = utils.generate_dict_for_analysis(analysis_types)
    

    for ebook_idx, ebook_name in enumerate(os.listdir(ebooks_path)):
        if ebook_idx > (n_ebooks - 1):
            break

        utils.analyse_ebook(f"{ebooks_path}/{ebook_name}", analysis_dict)


    # Serialization to json
    if to_json:
        utils.analysis_to_json(analysis_dict, f"{OUTPUT_PATH}/seq")

def load_and_analysis_par(ebooks_path:str, n_ebooks:int, n_process:int, analysis_types:dict, to_json:bool = False):
    
    # Initialization of dictionaries based on analysis
    analysis_dict = utils.generate_dict_for_analysis(analysis_types)

    # Equal distribution of ebook to process
    ebook_path_for_process_list = utils.distribute_ebooks_equally(ebooks_path, n_ebooks, n_process)

    # Creating, starting and joining processes
    processes = []
    result_queue = multiprocessing.Queue()
    for process_idx in range(n_process):
        processes.append(multiprocessing.Process(target=analyse_ebook_multiprocess, args=[ebook_path_for_process_list[process_idx], analysis_dict, result_queue]))    
    
    for process in processes:
        process.start()
    
    # Merging partial analysis
    merged_analysis = {}
    for _ in range(n_process):
        partial_analysis = result_queue.get()
        merged_analysis = utils.merge_analysis(merged_analysis, partial_analysis)

    # Serialization to json
    if to_json:
        utils.analysis_to_json(merged_analysis, f"{OUTPUT_PATH}/multi_processing")

async def load_and_analysis_asyncio(ebooks_path:str, n_ebooks:int, n_tasks:int, analysis_types:dict, to_json:bool = False):
    # Initialization of dictionaries based on analysis
    analysis_dict = utils.generate_dict_for_analysis(analysis_types)

    analysis_dict_list = [copy.deepcopy(analysis_dict) for _ in range(n_tasks)]

    # Equal distribution of ebook to task
    ebook_path_for_task_list = utils.distribute_ebooks_equally(ebooks_path, n_ebooks, n_tasks)
    
    # Create task list to run concurrently
    task_list = [analyse_ebook_asyncio(ebook_path_for_task_list[task_idx], analysis_dict_list[task_idx])  for task_idx in range(n_tasks)]
    await asyncio.gather(*task_list)

    # Merging partial analysis
    merged_analysis = {}
    for task_idx in range(n_tasks):
        merged_analysis = utils.merge_analysis(merged_analysis, analysis_dict_list[task_idx])

    # Serialization to json
    if to_json:
        utils.analysis_to_json(merged_analysis, f"{OUTPUT_PATH}/asyncio")

def load_and_analysis_multithreads(ebooks_path:str, n_ebooks:int, n_threads:int, analysis_types:dict, to_json:bool = False):
    # Initialization of dictionaries based on analysis
    analysis_dict = utils.generate_dict_for_analysis(analysis_types)

    analysis_dict_list = [copy.deepcopy(analysis_dict) for _ in range(n_threads)]

    # Equal distribution of ebook to thread
    ebook_path_for_thread_list = utils.distribute_ebooks_equally(ebooks_path, n_ebooks, n_threads)
    
    # Create, run and join threads
    threads = [threading.Thread(target=analyse_ebook_multithreads, args=(ebook_path_list, analysis_dict)) for ebook_path_list, analysis_dict in zip(ebook_path_for_thread_list, analysis_dict_list)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    
    # Merging partial analysis
    merged_analysis = {}
    for thread_idx in range(n_threads):
        merged_analysis = utils.merge_analysis(merged_analysis, analysis_dict_list[thread_idx])

    # Serialization to json
    if to_json:
        utils.analysis_to_json(merged_analysis, f"{OUTPUT_PATH}/multi_threading")

def benchmark(benchmark_config:dict):
    benchmark_results = []
    for _ in range(len(benchmark_config["process_num"])):
        benchmark_results.append(
            {
                "ebook_num" : 0,
                "process_num" : 0,
                "seq_timings" : [],
                "par_timings" : []
            })
        
    for test_idx, (process_n, ebook_n) in enumerate(zip(benchmark_config["process_num"], benchmark_config["ebook_num"])):
        benchmark_results[test_idx]["process_num"] = process_n
        benchmark_results[test_idx]["ebook_num"] = ebook_n
        # Repeat test sample_num times to obtain more reliable result
        for sample_n in range(benchmark_config["sample_num"]):
            # Benchmark of seq analysis
            if benchmark_config["seq_analysis"][test_idx]:
                print(f"Seq [{sample_n + 1}/{benchmark_config['sample_num']}] of [{test_idx + 1}/{len(benchmark_config['process_num'])}]", end='\r')
                start = time.clock_gettime(time.CLOCK_REALTIME)
                load_and_analysis_seq(f"{PARENT_PATH}/text_data", ebook_n, False, True)
                end = time.clock_gettime(time.CLOCK_REALTIME)
                benchmark_results[test_idx]["seq_timings"].append(end - start)

            # Benchmark of par analysis
            if benchmark_config["par_analysis"][test_idx]:
                print(f"Par [{sample_n + 1}/{benchmark_config['sample_num']}] of [{test_idx + 1}/{len(benchmark_config['process_num'])}]", end='\r')
                start = time.clock_gettime(time.CLOCK_REALTIME)
                load_and_analysis_par(f"{PARENT_PATH}/text_data", ebook_n, process_n, False, True)
                end = time.clock_gettime(time.CLOCK_REALTIME)
                benchmark_results[test_idx]["par_timings"].append(end - start)
    
    # Serialize to json
    with open(benchmark_config["json_output_path"], 'w') as json_file:
        json.dump(benchmark_results, json_file, indent=4)

def main():

    analysis_types = {
        "bigram_char"  :  True,
        "trigram_char" :  False,
        "bigram_word"  :  False,
        "trigram_word" :  False
    }

    # SEQ
    start = time.clock_gettime(time.CLOCK_REALTIME)
    load_and_analysis_seq(f"{PARENT_PATH}/text_data", 10, analysis_types, True)
    end = time.clock_gettime(time.CLOCK_REALTIME)
    print(f"Seq time : {end - start}")

    
    # Multiprocessing
    start = time.clock_gettime(time.CLOCK_REALTIME)
    load_and_analysis_par(f"{PARENT_PATH}/text_data", 10, 4, analysis_types, True)
    end = time.clock_gettime(time.CLOCK_REALTIME)
    print(f"Multiprocessing time : {end - start}")

    # Asyncio
    start = time.clock_gettime(time.CLOCK_REALTIME)
    asyncio.run(load_and_analysis_asyncio(f"{PARENT_PATH}/text_data", 10, 4, analysis_types, True))
    end = time.clock_gettime(time.CLOCK_REALTIME)
    print(f"Asyncio time : {end - start}")

    # Multithreading
    start = time.clock_gettime(time.CLOCK_REALTIME)
    load_and_analysis_multithreads(f"{PARENT_PATH}/text_data", 10, 4, analysis_types, True)
    end = time.clock_gettime(time.CLOCK_REALTIME)
    print(f"Multithreading time : {end - start}")



    # check_diff(f"{OUTPUT_PATH}/bigram_char_seq.json", f"{OUTPUT_PATH}/bigram_char_par.json")
    # check_diff(f"{OUTPUT_PATH}/trigram_char_seq.json", f"{OUTPUT_PATH}/trigram_char_par.json")
    # check_diff(f"{OUTPUT_PATH}/bigram_word_seq.json", f"{OUTPUT_PATH}/bigram_word_par.json")
    # check_diff(f"{OUTPUT_PATH}/trigram_word_seq.json", f"{OUTPUT_PATH}/trigram_word_par.json")
   
    # benchmark_config={
    #     "process_num" : [4, 4,  4,  4,  4],
    #     "ebook_num"  : [10,100,200,500,1000],
    #     "json_output_path" : f"{OUTPUT_PATH}/benchmarks_ebooks.json",
    #     "sample_num" : 5, 
    #     "seq_analysis" : [True,
    #     "par_analysis" : True
    # }

    # benchmark_config={
    #     "process_num" : [x for x in range(1,11)],
    #     "ebook_num"  : [500 for _ in range(1,11)],
    #     "json_output_path" : f"{OUTPUT_PATH}/benchmarks_process.json",
    #     "sample_num" : 5,
    #     "seq_analysis" : [True, False, False, False, False, False, False, False, False, False],
    #     "par_analysis" : [True for _ in range(1,11)]
    # }
    
    # benchmark(benchmark_config)

    



if __name__ == "__main__":
    main()
