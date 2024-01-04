import os
import json
import re
from collections import defaultdict


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

def merge_analysis(merged_analysis:dict, analysis_to_add:dict):

    if not merged_analysis:
        merged_analysis = analysis_to_add
    else:
        for analysis_type in analysis_to_add:
            if analysis_to_add[analysis_type] != None:
                if analysis_type == "bigram_char":
                    for key, value in analysis_to_add[analysis_type].items():
                        merged_analysis[analysis_type][key] += int(value)
                elif analysis_type == "trigram_char":
                    for key, value in analysis_to_add[analysis_type].items():
                        merged_analysis[analysis_type][key] += int(value)
                elif analysis_type == "bigram_word":
                    for first_key, first_value in analysis_to_add[analysis_type].items():
                        for second_key, second_value in first_value.items():
                            merged_analysis[analysis_type][first_key][second_key] += int(second_value)
                elif analysis_type == "trigram_word":
                    for first_key, first_value in analysis_to_add[analysis_type].items():
                        for second_key, second_value in first_value.items():
                            for third_key, third_value in second_value.items():
                                merged_analysis[analysis_type][first_key][second_key][third_key] += int(third_value)    
    return merged_analysis

def analysis_to_json(analysis:dict, json_output_folder_path:str):
    for type, analysis_data in analysis.items():
            if analysis_data != None:
                with open(f"{json_output_folder_path}/{type}.json", 'w') as json_file:
                    json.dump(analysis_data, json_file, indent=4)

def generate_dict_for_analysis(analysis_types:dict):
    analysis_dict = {
                        "bigram_char"  : None,
                        "trigram_char" : None,
                        "bigram_word"  : None,
                        "trigram_word" : None
                    }

    if analysis_types["bigram_char"]:
        analysis_dict["bigram_char"] = defaultdict(int)
    if analysis_types["trigram_char"]:
        analysis_dict["trigram_char"] = defaultdict(int)
    if analysis_types["bigram_word"]:
        analysis_dict["bigram_word"] = defaultdict(lambda: defaultdict(int))
    if analysis_types["trigram_word"]:
        analysis_dict["trigram_word"] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    return analysis_dict

def distribute_ebooks_equally(ebooks_path:str, n_ebooks:int, n_groups:int):
    ebook_path_list = [f"{ebooks_path}/{ebook_name}" for ebook_idx, ebook_name in enumerate(os.listdir(ebooks_path)) if ebook_idx < n_ebooks]

    ebook_path_list = sorted(ebook_path_list, key=lambda ebook_path: os.path.getsize(ebook_path))
    
    ebook_path_for_group_list = [[] for _ in range(n_groups)]

    for ebook_idx, ebook_path in enumerate(ebook_path_list):
        ebook_path_for_group_list[ebook_idx % n_groups].append(ebook_path)

    return ebook_path_for_group_list

def analyse_ebook(ebook_path:str, analysis_dict:dict):

    cleaned_text = read_and_filter_ebook(ebook_path)
    periods = cleaned_text.split('. ')
    
    for type, analysis_data in analysis_dict.items():
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

