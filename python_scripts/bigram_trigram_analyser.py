import os
import json
import re
from collections import defaultdict
import time


ABS_PATH = os.path.abspath(__file__)
PARENT_PATH = os.path.abspath(os.path.join(os.path.dirname(ABS_PATH), os.pardir))
TEXT_DATA_PATH = f"{PARENT_PATH}/text_data"
OUTPUT_PATH = f"{PARENT_PATH}/output/python_version"

def analyse_ebook(text_chunk:str, analysis_types:dict):


    periods = text_chunk.split('. ')
    # periods.remove("")
    
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


def load_and_analysis_seq(ebooks_path, to_json:bool = False, bigram_char:bool = False, trigram_char:bool = False, bigram_word:bool = False, trigram_word:bool = False):

    base_filter = re.compile(r'[^a-zA-Z \n.]')
    dots_filter = re.compile(r'\. \. \.')
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
    

    for ebook_name in os.listdir(ebooks_path):
        with open(f"{ebooks_path}/{ebook_name}") as ebook:
            ebook_text = ebook.read()

        
        cleaned_text = base_filter.sub("", ebook_text)
        cleaned_text = dots_filter.sub(" ", cleaned_text)
        cleaned_text = cleaned_text.replace('\n', " ")
        cleaned_text = cleaned_text.lower()
        
        analyse_ebook(cleaned_text, analysis_types)

    if to_json:
        for type, analysis_data in analysis_types.items():
            if analysis_data != None:
                with open(f"{OUTPUT_PATH}/{type}_seq.json", 'w') as json_file:
                    json.dump(analysis_data, json_file, indent=4)

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

def main():
    start = time.clock_gettime(time.CLOCK_REALTIME)
    load_and_analysis_seq(f"{PARENT_PATH}/debug_text_data", True, True, True, True, True)
    end = time.clock_gettime(time.CLOCK_REALTIME)
    print(end - start)

    # check_diff(f"{OUTPUT_PATH}/bigram_char_seq.json", f"{PARENT_PATH}/output/cpp_version/bigramCharSeq.json")
    # check_diff(f"{OUTPUT_PATH}/trigram_char_seq.json", f"{PARENT_PATH}/output/cpp_version/trigramCharSeq.json")
    # check_diff(f"{OUTPUT_PATH}/bigram_word_seq.json", f"{PARENT_PATH}/output/cpp_version/bigramWordSeq.json")
    # check_diff(f"{OUTPUT_PATH}/trigram_word_seq.json", f"{PARENT_PATH}/output/cpp_version/trigramWordSeq.json")


if __name__ == "__main__":
    main()
