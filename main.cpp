#include <omp.h>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <unordered_map>

namespace fs = std::filesystem;

// Bigram word to json
void analysisToJsonFile(std::string json_path, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> *bigram_occurrences){
    std::ofstream json_file(json_path, std::ios::out);

    json_file << "{\n";

    for(auto iter = bigram_occurrences->begin(); iter != bigram_occurrences->end(); iter++)
    {
        json_file << "\t\"" << iter->first << "\" : {\n";
        for(auto iter2 = iter->second.begin(); iter2 != iter->second.end(); iter2++)
        {
            json_file << "\t\t\"" << iter2->first << "\" : " << iter2->second;
            if(std::next(iter2) != iter->second.end())
            {
                json_file << " ,\n";
            }
        }
        json_file << "\n\t}";
        if(std::next(iter) != bigram_occurrences->end())
        {
            json_file << ",";
        }
        json_file << "\n";
    }

    json_file << "}";

}
// Trigram word to json
void analysisToJsonFile(std::string json_path, std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> *trigram_occurrences){
    std::ofstream json_file(json_path, std::ios::out);

    json_file << "{\n";

    for(auto iter = trigram_occurrences->begin(); iter != trigram_occurrences->end(); iter++)
    {
        json_file << "\t\"" << iter->first << "\" : {\n";
        for(auto iter2 = iter->second.begin(); iter2 != iter->second.end(); iter2++)
        {
            json_file << "\t\t\"" << iter2->first << "\" : {\n";
            for(auto iter3 = iter2->second.begin(); iter3 != iter2->second.end(); iter3++)
            {
                json_file << "\t\t\t\"" << iter3->first << "\" : " << iter3->second;
                if(std::next(iter3) != iter2->second.end())
                {
                    json_file << " ,\n";
                }
            }
            json_file << "\n\t\t}";
            if(std::next(iter2) != iter->second.end())
            {
                json_file << ",";
            }
            json_file << "\n";
            
        }
        json_file << "\n\t}";
        if(std::next(iter) != trigram_occurrences->end())
        {
            json_file << ",";
        }
        json_file << "\n";
    }

    json_file << "}";

}
// Bigram or Trigram to json
void analysisToJsonFile(std::string json_path, std::unordered_map<std::string, uint32_t>* occurrences){
    std::ofstream json_file(json_path, std::ios::out);

    json_file << "{\n";

    for(auto iter = occurrences->begin(); iter != occurrences->end(); iter++)
    {
        json_file << "\t\"" << iter->first << "\" : "<< iter->second;
        
        if(std::next(iter) != occurrences->end())
        {
            json_file << ",";
        }
        json_file << "\n";
    }

    json_file << "}";

}

inline std::string filterString(std::string str_to_filter, bool* need_to_break = nullptr)
{
    std::string str_filtered = "";

    for(int char_idx=0; char_idx<str_to_filter.length();char_idx++)
    {
        if( 97 <= int(str_to_filter[char_idx]) && int(str_to_filter[char_idx]) <= 122)
        {
            str_filtered += str_to_filter[char_idx];
        }
        else if( 65 <= int(str_to_filter[char_idx]) && int(str_to_filter[char_idx]) <= 90)
        {
            str_filtered += std::tolower(str_to_filter[char_idx]);
        }
        if(need_to_break != nullptr && char_idx == str_to_filter.length() - 1 && int(str_to_filter[char_idx]) == 46)
        {
            // notify to break analysis count if period is found
            *need_to_break = true;
        }
    }
    
    return str_filtered;

}

void bigramWordAnalyseChunk(std::string* chunk_ptr, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>*  bigram_occurrences)
{
    std::stringstream ss(*chunk_ptr);
    
    std::string current_word="", first_word="";
    while (std::getline(ss, current_word, ' '))
    {
        bool need_to_break = false;
        std::string word_filtered = filterString(current_word, &need_to_break);

        if(word_filtered != "")
        {
            if(first_word == "")
            {
                if(need_to_break)
                {
                    first_word = "";
                }
                else
                {
                    first_word = word_filtered;
                }
            }
            else
            {
                (*bigram_occurrences)[first_word][word_filtered]++;
                if(need_to_break)
                {
                    first_word = "";
                }
                else
                {
                    first_word = word_filtered;
                }
            }
        }
    }
}

void trigramWordAnalyseChunk(std::string* chunk_ptr, std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>>*  trigram_occurrences)
{
    std::stringstream ss(*chunk_ptr);
    
    std::string current_word="", first_word="", second_word="";
    while (std::getline(ss, current_word, ' '))
    {
        bool need_to_break = false;
        std::string word_filtered = filterString(current_word, &need_to_break);

        if(word_filtered != "")
        {
            if(first_word == "")
            {
                if(need_to_break)
                {
                    first_word = "";
                }
                else
                {
                    first_word = word_filtered;
                }
            }
            else if(second_word == "")
            {
                if(need_to_break)
                {
                    first_word = "";
                    second_word = "";
                }
                else
                {
                    second_word = word_filtered;
                }
            }
            else
            {
                (*trigram_occurrences)[first_word][second_word][word_filtered]++;
                if(need_to_break)
                {
                    first_word = "";
                    second_word = "";
                }
                else
                {
                    first_word = second_word;
                    second_word = word_filtered;
                }
            }
        }
    }
}

void bigramCharAnalyseChunk(std::string* chunk_ptr, std::unordered_map<std::string, uint32_t>* bigram_occurrences)
{
    std::stringstream ss(*chunk_ptr);
    
    std::string current_word="";
    while (std::getline(ss, current_word, ' '))
    {
        std::string word_filtered = filterString(current_word);

        if(word_filtered != "")
        {
            std::string bigram;
            for(int char_idx=0; char_idx<word_filtered.length(); char_idx++)
            {
                if(char_idx == 0)
                {
                    bigram = word_filtered[char_idx];
                }
                else
                {
                    bigram += word_filtered[char_idx];
                    (*bigram_occurrences)[bigram]++;
                    bigram = bigram[1];
                }
            }
        }
    }
}

void trigramCharAnalyseChunk(std::string* chunk_ptr, std::unordered_map<std::string, uint32_t>* trigram_occurrences)
{
    std::stringstream ss(*chunk_ptr);
    
    std::string current_word="";
    while (std::getline(ss, current_word, ' '))
    {
        std::string word_filtered = filterString(current_word);

        if(word_filtered != "")
        {
            std::string trigram;
            for(int char_idx=0; char_idx<word_filtered.length(); char_idx++)
            {
                if(char_idx == 0)
                {
                    trigram = word_filtered[char_idx];
                }
                else if(char_idx == 1){
                    trigram += word_filtered[char_idx];
                }
                else
                {
                    trigram += word_filtered[char_idx];
                    (*trigram_occurrences)[trigram]++;
                    trigram = trigram.substr(1,3);
                }
            }
        }
    }
}

void LoadAndAnalysisSeq(std::string text_data_path, int max_doc_num=-1, bool serialize_to_json=false)
{
    // Save path in a vector
    std::vector<fs::path> doc_paths;

    if (std::filesystem::exists(text_data_path) && std::filesystem::is_directory(text_data_path))
    {
        for (const auto& entry : std::filesystem::directory_iterator(text_data_path)) 
        {
            if(doc_paths.size() >= max_doc_num)
            {
                break;
            }
            else if(std::filesystem::is_regular_file(entry))
            {
                doc_paths.push_back(entry.path());
            }
        }
    } 
    else 
    {
        std::cerr << "The specified directory does not exist or is not a directory." << std::endl;
    }
    
    // Iter through file and load data 

    std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> bigram_word_occurrences;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> trigram_word_occurrences;
    std::unordered_map<std::string, uint32_t> bigram_char_occurrences;
    std::unordered_map<std::string, uint32_t> trigram_char_occurrences;

    
    for(auto iter_doc_path=doc_paths.begin(); iter_doc_path != doc_paths.end(); iter_doc_path++)
    {
        std::string doc_str;
        doc_str.resize(fs::file_size(*iter_doc_path));
        doc_str.clear();

        std::ifstream doc_input(*iter_doc_path, std::ios::in);
        if(!doc_input.is_open())
        {
            std::cout << *iter_doc_path << " not opened" << std::endl;
            // TODO handle opening error
        }

        std::string line;

        while(std::getline(doc_input, line))
        {

            if (line.back() == '\n')
            {
                line.pop_back();
            }    
            else if (line.back() == '\r')
            {
                line.pop_back();
                line.push_back(' ');
            }
            doc_str += line;
            
        }

        // Analyse chunk
        bigramWordAnalyseChunk(&doc_str, &bigram_word_occurrences);
        trigramWordAnalyseChunk(&doc_str, &trigram_word_occurrences);
        bigramCharAnalyseChunk(&doc_str, &bigram_char_occurrences);
        trigramCharAnalyseChunk(&doc_str, &trigram_char_occurrences);
    }

    // Serialize to Json
    if(serialize_to_json)
    {
        analysisToJsonFile("./../output/bigramWordSeq.json", &bigram_word_occurrences);
        analysisToJsonFile("./../output/trigramWordSeq.json", &trigram_word_occurrences);
        analysisToJsonFile("./../output/bigramCharSeq.json", &bigram_char_occurrences);
        analysisToJsonFile("./../output/trigramCharSeq.json", &trigram_char_occurrences);
    }
}

void LoadAndAnalysisPar(std::string text_data_path, int max_doc_num=-1, bool serialize_to_json=false)
{
    ssize_t total_docs_size = 0;
    // Save path in a vector
    std::vector<fs::path> doc_paths;

    if (std::filesystem::exists(text_data_path) && std::filesystem::is_directory(text_data_path))
    {
        for (const auto& entry : std::filesystem::directory_iterator(text_data_path)) 
        {   
            if(doc_paths.size() >= max_doc_num)
            {
                break;
            }
            else if (std::filesystem::is_regular_file(entry))
            {
                doc_paths.push_back(entry.path());
                total_docs_size += entry.file_size();
            }
        }
    } 
    else 
    {
        std::cerr << "The specified directory does not exist or is not a directory." << std::endl;
    }
    
    uint32_t max_threads = omp_get_max_threads();

    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> thread_bigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>>> thread_trigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_bigram_char_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_trigram_char_occurrences(max_threads);

    std::vector<std::string> thread_docs_str(doc_paths.size());
    std::vector<bool> doc_ready(doc_paths.size(), false);
    
    #pragma omp parallel shared(doc_paths, thread_docs_str, thread_bigram_word_occurrences, thread_trigram_word_occurrences, thread_bigram_char_occurrences, thread_trigram_char_occurrences)
    {
        #pragma omp single
        {
            // Iter through file and save data
            uint32_t doc_idx = 0;
            for(auto iter_doc_path=doc_paths.begin(); iter_doc_path != doc_paths.end(); iter_doc_path++)
            {
                thread_docs_str[doc_idx].resize(fs::file_size(*iter_doc_path));

                std::ifstream doc_input(*iter_doc_path, std::ios::in);
                if(!doc_input.is_open())
                {
                    std::cout << *iter_doc_path << " not opened" << std::endl;
                    // TODO handle opening error
                }

                std::string line;

                while(std::getline(doc_input, line))
                {

                    if (line.back() == '\n')
                    {
                        line.pop_back();
                    }    
                    else if (line.back() == '\r')
                    {
                        line.pop_back();
                        line.push_back(' ');
                    }
                    thread_docs_str[doc_idx] += line;
                    
                }
                doc_ready[doc_idx] = true;
                doc_idx++;
            }
        }

        uint32_t doc_count = 0;
        uint32_t thread_idx = 0;
        uint32_t thread_doc_idx = 0;
        
        while(true)
        {
            thread_idx = omp_get_thread_num();
            thread_doc_idx = doc_count * (max_threads) + thread_idx;

            if(thread_doc_idx >= doc_paths.size())
            {
                break;
            }
            else
            {
                while(!doc_ready[thread_doc_idx])
                {
                    // Wait until doc is ready to be processed
                }

                
                // Analyse chunk
                bigramWordAnalyseChunk(&thread_docs_str[thread_doc_idx], &thread_bigram_word_occurrences[thread_idx]);
                trigramWordAnalyseChunk(&thread_docs_str[thread_doc_idx], &thread_trigram_word_occurrences[thread_idx]);
                bigramCharAnalyseChunk(&thread_docs_str[thread_doc_idx], &thread_bigram_char_occurrences[thread_idx]);
                trigramCharAnalyseChunk(&thread_docs_str[thread_doc_idx], &thread_trigram_char_occurrences[thread_idx]);
                doc_count ++;
            }
        }

    }

    // Merge partial maps
    
    std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> total_bigram_word_occurrences;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> total_trigram_word_occurrences;
    std::unordered_map<std::string, uint32_t> total_bigram_char_occurrences;
    std::unordered_map<std::string, uint32_t> total_trigram_char_occurrences;

    for(int thread_idx=0; thread_idx < max_threads; thread_idx++)
    {
        // bigram word merge
        for(auto iter_first_word : thread_bigram_word_occurrences[thread_idx])
        {
            for(auto iter_second_word : iter_first_word.second)
            {
                total_bigram_word_occurrences[iter_first_word.first][iter_second_word.first] += iter_second_word.second;
            }
        }
        // trigram word merge
        for(auto iter_first_word : thread_trigram_word_occurrences[thread_idx])
        {
            for(auto iter_second_word : iter_first_word.second)
            {
                for(auto iter_third_word : iter_second_word.second)
                {
                    total_trigram_word_occurrences[iter_first_word.first][iter_second_word.first][iter_third_word.first] += iter_third_word.second;
                }
            }
        }
        // bigram char merge
        for(auto iter_char : thread_bigram_char_occurrences[thread_idx])
        {
            total_bigram_char_occurrences[iter_char.first] += iter_char.second;
        }
        // trigram char merge
        for(auto iter_char : thread_trigram_char_occurrences[thread_idx])
        {
            total_trigram_char_occurrences[iter_char.first] += iter_char.second;
        }
    }

    // Serialize to Json
    if(serialize_to_json)
    {
        analysisToJsonFile("./../output/bigramWordPar.json", &total_bigram_word_occurrences);
        analysisToJsonFile("./../output/trigramWordPar.json", &total_trigram_word_occurrences);
        analysisToJsonFile("./../output/bigramCharPar.json", &total_bigram_char_occurrences);
        analysisToJsonFile("./../output/trigramCharPar.json", &total_trigram_char_occurrences);
    }
    
}

void LoadAndAnalysisParV2(std::string text_data_path, int max_doc_num=-1, bool serialize_to_json=false)
{
    // Save paths in a vector
    std::vector<fs::path> doc_paths;

    if (std::filesystem::exists(text_data_path) && std::filesystem::is_directory(text_data_path))
    {
        for (const auto& entry : std::filesystem::directory_iterator(text_data_path)) 
        {   
            if(doc_paths.size() >= max_doc_num)
            {
                break;
            }
            else if (std::filesystem::is_regular_file(entry))
            {
                doc_paths.push_back(entry.path());
            }
        }
    } 
    else 
    {
        std::cerr << "The specified directory does not exist or is not a directory." << std::endl;
    }
    
    uint32_t max_threads = omp_get_max_threads();

    // Split doc_path based on max threads

    uint32_t num_doc_for_thread = doc_paths.size() / max_threads;
    uint32_t doc_remainder = doc_paths.size() % max_threads;
    std::vector<std::vector<fs::path>> threads_doc_paths;
    uint32_t start_doc_idx = 0;
    for(int thread_idx=0; thread_idx<max_threads; thread_idx++)
    {
        uint32_t end_doc_idx = start_doc_idx + num_doc_for_thread - 1;
        if(doc_remainder > 0)
        {
            end_doc_idx++;
            doc_remainder--;
        }

        std::vector<fs::path> doc_subset(doc_paths.begin() + start_doc_idx, doc_paths.begin() + end_doc_idx + 1);
        threads_doc_paths.push_back(doc_subset);

        start_doc_idx = end_doc_idx + 1;
    }


    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> thread_bigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>>> thread_trigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_bigram_char_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_trigram_char_occurrences(max_threads);

    std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> total_bigram_word_occurrences;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> total_trigram_word_occurrences;
    std::unordered_map<std::string, uint32_t> total_bigram_char_occurrences;
    std::unordered_map<std::string, uint32_t> total_trigram_char_occurrences;

    #pragma omp parallel shared(threads_doc_paths, thread_bigram_word_occurrences, thread_trigram_word_occurrences, thread_bigram_char_occurrences, thread_trigram_char_occurrences)
    {
        uint32_t thread_idx = omp_get_thread_num();

        if(threads_doc_paths[thread_idx].size() != 0 )
        {
            for(auto iter_doc_path=threads_doc_paths[thread_idx].begin(); iter_doc_path != threads_doc_paths[thread_idx].end(); iter_doc_path++)
            {
                std::string doc_str;
                doc_str.resize(fs::file_size(*iter_doc_path));
                doc_str.clear();

                std::ifstream doc_input(*iter_doc_path, std::ios::in);
                if(!doc_input.is_open())
                {
                    std::cout << *iter_doc_path << " not opened" << std::endl;
                    // TODO handle opening error
                }

                std::string line;

                while(std::getline(doc_input, line))
                {

                    if (line.back() == '\n')
                    {
                        line.pop_back();
                    }    
                    else if (line.back() == '\r')
                    {
                        line.pop_back();
                        line.push_back(' ');
                    }
                    doc_str += line;
                    
                }

                // Analyse chunk
                bigramWordAnalyseChunk(&doc_str, &thread_bigram_word_occurrences[thread_idx]);
                trigramWordAnalyseChunk(&doc_str, &thread_trigram_word_occurrences[thread_idx]);
                bigramCharAnalyseChunk(&doc_str, &thread_bigram_char_occurrences[thread_idx]);
                trigramCharAnalyseChunk(&doc_str, &thread_trigram_char_occurrences[thread_idx]);
            
            }
        }

        #pragma omp barrier
        // Merge partial unordered_maps
        // Assume at least 4 thread TODO: check

        for(int partial_map_idx=0; partial_map_idx < max_threads; partial_map_idx++)
        {
            if(thread_idx == 0)
            {
                // bigram word merge
                for(auto iter_first_word : thread_bigram_word_occurrences[partial_map_idx])
                {
                    for(auto iter_second_word : iter_first_word.second)
                    {
                        total_bigram_word_occurrences[iter_first_word.first][iter_second_word.first] += iter_second_word.second;
                    }
                }
            }
            else if(thread_idx == 1)
            {
                // trigram word merge
                for(auto iter_first_word : thread_trigram_word_occurrences[partial_map_idx])
                {
                    for(auto iter_second_word : iter_first_word.second)
                    {
                        for(auto iter_third_word : iter_second_word.second)
                        {
                            total_trigram_word_occurrences[iter_first_word.first][iter_second_word.first][iter_third_word.first] += iter_third_word.second;
                        }
                    }
                }
            }
            else if(thread_idx == 2)
            {
                // bigram char merge
                for(auto iter_char : thread_bigram_char_occurrences[partial_map_idx])
                {
                    total_bigram_char_occurrences[iter_char.first] += iter_char.second;
                }
            }
            else if(thread_idx == 3)
            {
                // trigram char merge
                for(auto iter_char : thread_trigram_char_occurrences[partial_map_idx])
                {
                    total_trigram_char_occurrences[iter_char.first] += iter_char.second;
                }
            }
            else
            {
                break;
            }
        }
    
    }

    // Serialize to Json
    if(serialize_to_json)
    {
        analysisToJsonFile("./../output/bigramWordParV2.json", &total_bigram_word_occurrences);
        analysisToJsonFile("./../output/trigramWordParV2.json", &total_trigram_word_occurrences);
        analysisToJsonFile("./../output/bigramCharParV2.json", &total_bigram_char_occurrences);
        analysisToJsonFile("./../output/trigramCharParV2.json", &total_trigram_char_occurrences);
    }

}

void Benchmark(std::vector<uint32_t> ebooks_to_load)
{
    struct benchmarks_data
    {
        std::vector<int> ebook_num;
        std::vector<std::vector<double>> elapsed_seq, elapsed_par, elapsed_parV2;


        void ToJsonFile(std::string json_path)
        {
            int total_benchmark = ebook_num.size();
            std::ofstream json_file(json_path, std::ios::out);

            json_file << "[\n";

            for(int benchmark_idx=0; benchmark_idx<total_benchmark; benchmark_idx++)
            {
                json_file << "\t{\n";
                json_file << "\t\"ebook_num\" : " << ebook_num[benchmark_idx] <<",\n";
                json_file << "\t \"seq_timings\" : [ ";
                for(auto time_measure : elapsed_seq[benchmark_idx])
                {
                    json_file << time_measure;
                    if(time_measure != elapsed_seq[benchmark_idx][elapsed_seq[benchmark_idx].size() - 1])
                    {
                        json_file << " ,";
                    }
                     
                }
                json_file << " ],\n";

                json_file << "\t \"par_timings\" : [ ";
                for(auto time_measure : elapsed_par[benchmark_idx])
                {
                    json_file << time_measure;
                    if(time_measure != elapsed_par[benchmark_idx][elapsed_par[benchmark_idx].size() - 1])
                    {
                        json_file << " ,";
                    }
                }
                json_file << " ],\n";

                json_file << "\t \"par_timings_V2\" : [ ";
                for(auto time_measure : elapsed_parV2[benchmark_idx])
                {
                    json_file << time_measure;
                    if(time_measure != elapsed_parV2[benchmark_idx][elapsed_parV2[benchmark_idx].size() - 1])
                    {
                        json_file << " ,";
                    }
                }
                json_file << " ]\n";
                json_file << "\t}";
                if(benchmark_idx != total_benchmark-1)
                {
                    json_file << ",";
                }
                json_file << "\n";
            }

            json_file << "]";

        }
    };
    
    benchmarks_data benchmarks;
    uint32_t iter_for_reliability = 10;
    uint32_t main_benchmarks_number = ebooks_to_load.size();

    for(int main_benchmarks_idx=0; main_benchmarks_idx< main_benchmarks_number; main_benchmarks_idx++)
    {
        std::vector<double> elapsed_seq, elapsed_par, elapsed_parV2;
        double start_time=0, end_time=0;

        benchmarks.ebook_num.push_back(ebooks_to_load[main_benchmarks_idx]);

        for(int reliability_idx=0;reliability_idx<iter_for_reliability;reliability_idx++)
        {
            std::cout << "Seq_Analysis    : [ " << reliability_idx + 1 << " / " << iter_for_reliability << " ] of [ " 
                                             << main_benchmarks_idx + 1 << " / " << main_benchmarks_number << " ]" << std::flush;
            std::cout << "\r";
            start_time = omp_get_wtime();
            LoadAndAnalysisSeq("./../text_data", ebooks_to_load[main_benchmarks_idx]);
            end_time = omp_get_wtime();
            elapsed_seq.push_back(end_time-start_time);

            std::cout << "Par_Analysis    : [ " << reliability_idx + 1 << " / " << iter_for_reliability << " ] of [ "
                                             << main_benchmarks_idx + 1 << " / " << main_benchmarks_number << " ]" << std::flush;
            std::cout << "\r";
            start_time = omp_get_wtime();
            LoadAndAnalysisPar("./../text_data", ebooks_to_load[main_benchmarks_idx]);
            end_time = omp_get_wtime();
            elapsed_par.push_back(end_time-start_time);

            std::cout << "Par_Analysis_V2 : [ " << reliability_idx + 1 << " / " << iter_for_reliability << " ] of [ "
                                             << main_benchmarks_idx + 1 << " / " << main_benchmarks_number << " ]" << std::flush;
            std::cout << "\r";
            start_time = omp_get_wtime();
            LoadAndAnalysisParV2("./../text_data", ebooks_to_load[main_benchmarks_idx]);
            end_time = omp_get_wtime();
            elapsed_parV2.push_back(end_time-start_time);
        }
        
        benchmarks.elapsed_seq.push_back(elapsed_seq);
        benchmarks.elapsed_par.push_back(elapsed_par);
        benchmarks.elapsed_parV2.push_back(elapsed_parV2);
    }
    
    benchmarks.ToJsonFile("./../output/benchmarks.json");

}

int main() 
{

    // std::vector<uint32_t> ebooks_to_load = {10, 50, 100, 500, 1000};
    // std::vector<uint32_t> ebooks_to_load = {10, 50, 100, 500, 1000};
    // Benchmark(ebooks_to_load);

    // double start_time=0, end_time=0;
    // std::cout << "Seq" << "\n";
    // start_time = omp_get_wtime();
    // LoadAndAnalysisSeq("./../text_data", 100);
    // end_time = omp_get_wtime();
    // std::cout << "time :" << end_time -start_time << "\n";

    // std::cout << "Par" << "\n";
    // start_time = omp_get_wtime();
    // LoadAndAnalysisPar("./../text_data", 100);
    // end_time = omp_get_wtime();
    // std::cout << "time :" << end_time -start_time << "\n";

    // std::cout << "ParV2" << "\n";
    // start_time = omp_get_wtime();
    LoadAndAnalysisParV2("./../text_data", 100);
    // end_time = omp_get_wtime();
    // std::cout << "time :" << end_time -start_time << "\n";
}