import os
import json
import re
from collections import defaultdict
import time
import multiprocessing


ABS_PATH = os.path.abspath(__file__)
PARENT_PATH = os.path.abspath(os.path.join(os.path.dirname(ABS_PATH), os.pardir))
OUTPUT_PATH = f"{PARENT_PATH}/output/python_version"

def single_nested_defaultdict_int():
    return defaultdict(int)

def double_nested_defaultdict_int():
    return defaultdict(single_nested_defaultdict_int)

def check_diff(json_path_a:str, json_path_b:str):
    with open(json_path_a) as json_file_a:
        json_a = json.load(json_file_a)
    with open(json_path_b) as json_file_b:
        json_b = json.load(json_file_b)

    for key,value in json_a.items():
        if key not in json_b:
            print(f"Chiave {key} di {os.path.basename(json_path_a)} non presente in {os.path.basename(json_path_b)}")
        elif isinstance(value, dict):
            for key_second, value_second in value.items():
                if key_second not in json_b[key]:
                    print(f"Chiave {key} : {key_second} di {os.path.basename(json_path_a)} non presente in {os.path.basename(json_path_b)}")
                elif isinstance(value_second, dict):
                    for key_third, value_third in value_second.items():
                        if key_third not in json_b[key][key_second]:
                            print(f"Chiave {key} : {key_second} : {key_third} di {os.path.basename(json_path_a)} non presente in {os.path.basename(json_path_b)}")

    for key,value in json_b.items():
        if key not in json_a:
            print(f"Chiave {key} di {os.path.basename(json_path_b)} non presente in {os.path.basename(json_path_a)}")
        elif isinstance(value, dict):
            for key_second, value_second in value.items():
                if key_second not in json_a[key]:
                    print(f"Chiave {key} : {key_second} di {os.path.basename(json_path_b)} non presente in {os.path.basename(json_path_a)}")    
                elif isinstance(value_second, dict):
                    for key_third, value_third in value_second.items():
                        if key_third not in json_a[key][key_second]:
                            print(f"Chiave {key} : {key_second} : {key_third} di {os.path.basename(json_path_b)} non presente in {os.path.basename(json_path_a)}")

def read_and_filter_ebook(ebook_path:str):
    base_filter = re.compile(r'[^a-zA-Z \n.]')
    dots_filter = re.compile(r'\. \. \.')

    cleaned_text = ""

    with open(ebook_path, "r") as ebook:
        cleaned_text = ebook.read()
        cleaned_text = base_filter.sub("", cleaned_text)
        cleaned_text = dots_filter.sub(" ", cleaned_text)
        cleaned_text = cleaned_text.replace('\n', " ")
        cleaned_text = cleaned_text.lower()
    
    return cleaned_text

def analyse_ebook(ebook_path:str, analysis_types:dict):

    cleaned_text = read_and_filter_ebook(ebook_path)
    periods = cleaned_text.split('. ')
    
    for type, analysis_data in analysis_types.items():
            if analysis_data != None:
                if type == "bigram_char":
                    for period in periods:
                        for word in period.split(" "):
                            if word != '':
                                bigram = ''
                                for char_idx, char in enumerate(word):
                                    if char_idx == 0:
                                        bigram = char
                                    else:
                                        bigram += char
                                        analysis_data[bigram] += 1
                                        bigram = char

                elif type == "trigram_char":
                    for period in periods:
                        for word in period.split(" "):
                            if word != '':
                                trigram = ''
                                for char_idx, char in enumerate(word):
                                    if char_idx == 0:
                                        trigram = char
                                    elif char_idx == 1:
                                        trigram += char
                                    else:
                                        trigram += char
                                        analysis_data[trigram] += 1
                                        trigram = trigram[1:3]
                elif type == "bigram_word":
                    for period in periods:
                        first_word = ""
                        for word in period.split(" "):
                            if word != '':
                                if first_word == "":
                                    first_word = word
                                else:
                                    analysis_data[first_word][word] += 1
                                    first_word = word

                elif type == "trigram_word":
                    for period in periods:
                        first_word = ""
                        second_word = ""
                        for word in period.split(" "):
                            if word != '':
                                if first_word == "":
                                    first_word = word
                                elif second_word == "":
                                    second_word = word
                                else:
                                    analysis_data[first_word][second_word][word] += 1
                                    first_word = second_word
                                    second_word = word

def analyse_ebook_multiprocess(ebook_path_list:str, analysis_types:dict, result_queue:multiprocessing.Queue):
    for ebook_path in ebook_path_list:
        analyse_ebook(ebook_path, analysis_types)

    result_queue.put(analysis_types)
    
def load_and_analysis_seq(ebooks_path:str, n_ebooks:int,to_json:bool = False, bigram_char:bool = False, trigram_char:bool = False, bigram_word:bool = False, trigram_word:bool = False):

    # Initialization of dictionaries based on analysis
    analysis_types = {
                            "bigram_char"  : None,
                            "trigram_char" : None,
                            "bigram_word"  : None,
                            "trigram_word" : None
                       }

    if bigram_char:
        analysis_types["bigram_char"] = defaultdict(int)
    if trigram_char:
        analysis_types["trigram_char"] = defaultdict(int)
    if bigram_word:
        analysis_types["bigram_word"] = defaultdict(lambda: defaultdict(int))
    if trigram_word:
        analysis_types["trigram_word"] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    

    for ebook_idx, ebook_name in enumerate(os.listdir(ebooks_path)):
        if ebook_idx > (n_ebooks - 1):
            break

        analyse_ebook(f"{ebooks_path}/{ebook_name}", analysis_types)


    # Serialization to json
    if to_json:
        for type, analysis_data in analysis_types.items():
            if analysis_data != None:
                with open(f"{OUTPUT_PATH}/{type}_seq.json", 'w') as json_file:
                    json.dump(analysis_data, json_file, indent=4)

def load_and_analysis_par(ebooks_path:str, n_ebooks:int, n_process:int, to_json:bool = False, bigram_char:bool = False, trigram_char:bool = False, bigram_word:bool = False, trigram_word:bool = False):
    
    # Initialization of dictionaries based on analysis
    analysis_types = {
                            "bigram_char"  : None,
                            "trigram_char" : None,
                            "bigram_word"  : None,
                            "trigram_word" : None
                       }

    if bigram_char:
        analysis_types["bigram_char"] = defaultdict(int)
    if trigram_char:
        analysis_types["trigram_char"] = defaultdict(int)
    if bigram_word:
        analysis_types["bigram_word"] = defaultdict(single_nested_defaultdict_int)
    if trigram_word:
        analysis_types["trigram_word"] = defaultdict(double_nested_defaultdict_int)

    # Equal distribution of ebook to process
    ebook_path_list = [f"{ebooks_path}/{ebook_name}" for ebook_idx, ebook_name in enumerate(os.listdir(ebooks_path)) if ebook_idx < n_ebooks]

    ebook_path_list = sorted(ebook_path_list, key=lambda ebook_path: os.path.getsize(ebook_path))
    
    ebook_path_for_process_list = [[] for _ in range(n_process)]

    for ebook_idx, ebook_path in enumerate(ebook_path_list):
        ebook_path_for_process_list[ebook_idx % n_process].append(ebook_path)

    # Creating, starting and joining processes
    processes = []
    result_queue = multiprocessing.Queue()
    for process_idx in range(n_process):
        processes.append(multiprocessing.Process(target=analyse_ebook_multiprocess, args=[ebook_path_for_process_list[process_idx], analysis_types, result_queue]))    
    
    for process in processes:
        process.start()
    
    # Merging partial analysis
    merged_analysis = None
    for _ in range(n_process):
        partial_analysis = result_queue.get()
        if merged_analysis == None:
            merged_analysis = partial_analysis
        else:
            for analysis_type in partial_analysis:
                if partial_analysis[analysis_type] != None:
                    if analysis_type == "bigram_char":
                        for key, value in partial_analysis[analysis_type].items():
                            merged_analysis[analysis_type][key] += int(value)
                    elif analysis_type == "trigram_char":
                        for key, value in partial_analysis[analysis_type].items():
                            merged_analysis[analysis_type][key] += int(value)
                    elif analysis_type == "bigram_word":
                        for first_key, first_value in partial_analysis[analysis_type].items():
                            for second_key, second_value in first_value.items():
                                merged_analysis[analysis_type][first_key][second_key] += int(second_value)
                    elif analysis_type == "trigram_word":
                        for first_key, first_value in partial_analysis[analysis_type].items():
                            for second_key, second_value in first_value.items():
                                for third_key, third_value in second_value.items():
                                    merged_analysis[analysis_type][first_key][second_key][third_key] += int(third_value)    

    # Serialization to json
    if to_json:
        for type, analysis_data in merged_analysis.items():
            if analysis_data != None:
                with open(f"{OUTPUT_PATH}/{type}_par.json", 'w') as json_file:
                    json.dump(analysis_data, json_file, indent=4)

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
    # start = time.clock_gettime(time.CLOCK_REALTIME)
    # load_and_analysis_seq(f"{PARENT_PATH}/text_data", 500, False, True)
    # end = time.clock_gettime(time.CLOCK_REALTIME)
    # print(f"Seq time : {end - start}")

    # start = time.clock_gettime(time.CLOCK_REALTIME)
    # load_and_analysis_par(f"{PARENT_PATH}/text_data", 500, 4, False, True)
    # end = time.clock_gettime(time.CLOCK_REALTIME)
    # print(f"Par time : {end - start}")

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

    benchmark_config={
        "process_num" : [x for x in range(1,11)],
        "ebook_num"  : [500 for _ in range(1,11)],
        "json_output_path" : f"{OUTPUT_PATH}/benchmarks_process.json",
        "sample_num" : 5,
        "seq_analysis" : [True, False, False, False, False, False, False, False, False, False],
        "par_analysis" : [True for _ in range(1,11)]
    }
    
    benchmark(benchmark_config)




if __name__ == "__main__":
    main()
