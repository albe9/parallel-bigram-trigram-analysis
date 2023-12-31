#include <omp.h>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <regex>

#include "utils.h"

namespace fs = std::filesystem;


void LoadAndAnalysisSeq(std::string text_data_path, int32_t max_doc_num=-1, bool serialize_to_json=false,
                            bool bigram_char=false, bool trigram_char=false, bool bigram_word=false, bool trigram_word=false)
{
    // Save path in a vector
    std::vector<fs::path> doc_paths;
    LoadPaths(text_data_path, &doc_paths, max_doc_num);
    
    // Iter through file and load data 

    std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> bigram_word_occurrences;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> trigram_word_occurrences;
    std::unordered_map<std::string, uint32_t> bigram_char_occurrences;
    std::unordered_map<std::string, uint32_t> trigram_char_occurrences;

    
    for(auto& doc_path : doc_paths)
    {
        // Load doc
        std::string doc_str;
        LoadDoc(&doc_str, &doc_path);

        // Filter doc
        std::string doc_filtered;
        doc_filtered.reserve(sizeof(doc_str));
        filterDoc(&doc_str, &doc_filtered);


        // Analyse Doc
        if(bigram_word)
        bigramWordAnalyseChunk(&doc_filtered, &bigram_word_occurrences);
        if(trigram_word)
        trigramWordAnalyseChunk(&doc_filtered, &trigram_word_occurrences);
        if(bigram_char)
        bigramCharAnalyseChunk(&doc_filtered, &bigram_char_occurrences);
        if(trigram_char)
        trigramCharAnalyseChunk(&doc_filtered, &trigram_char_occurrences);
    }

    // Serialize to Json
    if(serialize_to_json)
    {
        if(bigram_word)
        analysisToJsonFile("./../output/cpp_version/bigramWordSeq.json", &bigram_word_occurrences);
        if(trigram_word)
        analysisToJsonFile("./../output/cpp_version/trigramWordSeq.json", &trigram_word_occurrences);
        if(bigram_char)
        analysisToJsonFile("./../output/cpp_version/bigramCharSeq.json", &bigram_char_occurrences);
        if(trigram_char)
        analysisToJsonFile("./../output/cpp_version/trigramCharSeq.json", &trigram_char_occurrences);
    }
}

void LoadAndAnalysisPar(std::string text_data_path, int32_t max_doc_num=-1, bool serialize_to_json=false,
                            bool bigram_char=false, bool trigram_char=false, bool bigram_word=false, bool trigram_word=false)
{

    ssize_t total_docs_size = 0;
    // Save path in a vector
    std::vector<fs::path> doc_paths;
    LoadPaths(text_data_path, &doc_paths, max_doc_num);
    
    // Sort the vector based on file sizes (to distribute them equally)
    std::sort(doc_paths.begin(), doc_paths.end(), [](const fs::path& path1, const fs::path& path2) {
        return fs::file_size(path1) < fs::file_size(path2);
    });
    
    uint32_t max_threads = omp_get_max_threads();

    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> thread_bigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>>> thread_trigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_bigram_char_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_trigram_char_occurrences(max_threads);

    std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> total_bigram_word_occurrences;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> total_trigram_word_occurrences;
    std::unordered_map<std::string, uint32_t> total_bigram_char_occurrences;
    std::unordered_map<std::string, uint32_t> total_trigram_char_occurrences;


    std::vector<std::string> thread_docs_str(doc_paths.size());
    std::vector<bool> doc_ready(doc_paths.size(), false);
    
    #pragma omp parallel shared(doc_paths, thread_docs_str, doc_ready, thread_bigram_word_occurrences, thread_trigram_word_occurrences, thread_bigram_char_occurrences, thread_trigram_char_occurrences, \
                                total_bigram_word_occurrences, total_trigram_word_occurrences, total_bigram_char_occurrences, total_trigram_char_occurrences)
    {
        #pragma omp single
        {
            // Iter through file and save data
            uint32_t doc_idx = 0;
            for(auto& doc_path : doc_paths)
            {
                // Load Doc into thread's string
                LoadDoc(&thread_docs_str[doc_idx], &doc_path);
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

                // Filter doc
                std::string doc_filtered;
                doc_filtered.reserve(sizeof(thread_docs_str[thread_doc_idx]));
                filterDoc(&thread_docs_str[thread_doc_idx], &doc_filtered);

                // Analyse Doc
                if(bigram_word)
                bigramWordAnalyseChunk(&doc_filtered, &thread_bigram_word_occurrences[thread_idx]);
                if(trigram_word)
                trigramWordAnalyseChunk(&doc_filtered, &thread_trigram_word_occurrences[thread_idx]);
                if(bigram_char)
                bigramCharAnalyseChunk(&doc_filtered, &thread_bigram_char_occurrences[thread_idx]);
                if(trigram_char)
                trigramCharAnalyseChunk(&doc_filtered, &thread_trigram_char_occurrences[thread_idx]);
                thread_docs_str[thread_doc_idx].clear();
                doc_filtered.clear();
                doc_count ++;
            }
        }
    
    #pragma omp barrier
        // Merge partial unordered_maps
        // Assume at least 4 thread TODO: check
    
        for(uint32_t partial_map_idx=0; partial_map_idx < max_threads; partial_map_idx++)
        {
            if(thread_idx == 0 && bigram_word)
            {
                // bigram word merge
                for(auto& iter_first_word : thread_bigram_word_occurrences[partial_map_idx])
                {
                    for(auto& iter_second_word : iter_first_word.second)
                    {
                        total_bigram_word_occurrences[iter_first_word.first][iter_second_word.first] += iter_second_word.second;
                    }
                }
            }
            else if(thread_idx == 1 && trigram_word)
            {
                // trigram word merge
                for(auto& iter_first_word : thread_trigram_word_occurrences[partial_map_idx])
                {
                    for(auto& iter_second_word : iter_first_word.second)
                    {
                        for(auto& iter_third_word : iter_second_word.second)
                        {
                            total_trigram_word_occurrences[iter_first_word.first][iter_second_word.first][iter_third_word.first] += iter_third_word.second;
                        }
                    }
                }
            }
            else if(thread_idx == 2 && bigram_char)
            {
                // bigram char merge
                for(auto& iter_char : thread_bigram_char_occurrences[partial_map_idx])
                {
                    total_bigram_char_occurrences[iter_char.first] += iter_char.second;
                }
            }
            else if(thread_idx == 3 && trigram_char)
            {
                // trigram char merge
                for(auto& iter_char : thread_trigram_char_occurrences[partial_map_idx])
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
        if(bigram_word)
        analysisToJsonFile("./../output/cpp_version/bigramWordPar.json", &total_bigram_word_occurrences);
        if(trigram_word)
        analysisToJsonFile("./../output/cpp_version/trigramWordPar.json", &total_trigram_word_occurrences);
        if(bigram_char)
        analysisToJsonFile("./../output/cpp_version/bigramCharPar.json", &total_bigram_char_occurrences);
        if(trigram_char)
        analysisToJsonFile("./../output/cpp_version/trigramCharPar.json", &total_trigram_char_occurrences);
    }
    
}

void LoadAndAnalysisParV2(std::string text_data_path, int32_t max_doc_num=-1, bool serialize_to_json=false,
                            bool bigram_char=false, bool trigram_char=false, bool bigram_word=false, bool trigram_word=false)
{
    // Save paths in a vector
    std::vector<fs::path> doc_paths;
    LoadPaths(text_data_path, &doc_paths, max_doc_num);
    
    // Sort the vector based on file sizes (to distribute them equally)
    std::sort(doc_paths.begin(), doc_paths.end(), [](const fs::path& path1, const fs::path& path2) {
        return fs::file_size(path1) < fs::file_size(path2);
    });

    
    uint32_t max_threads = omp_get_max_threads();

    // Split doc_path based on max threads
    std::vector<std::vector<fs::path>> threads_doc_paths(max_threads);
    uint32_t thread_idx = 0;
    for(auto doc_path : doc_paths)
    {
        threads_doc_paths[thread_idx % max_threads].push_back(doc_path);
        thread_idx++;
    }

    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> thread_bigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>>> thread_trigram_word_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_bigram_char_occurrences(max_threads);
    std::vector<std::unordered_map<std::string, uint32_t>> thread_trigram_char_occurrences(max_threads);

    std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> total_bigram_word_occurrences;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> total_trigram_word_occurrences;
    std::unordered_map<std::string, uint32_t> total_bigram_char_occurrences;
    std::unordered_map<std::string, uint32_t> total_trigram_char_occurrences;

    #pragma omp parallel shared(threads_doc_paths, thread_bigram_word_occurrences, thread_trigram_word_occurrences, thread_bigram_char_occurrences, thread_trigram_char_occurrences, \
                                total_bigram_word_occurrences, total_trigram_word_occurrences, total_bigram_char_occurrences, total_trigram_char_occurrences)
    {
        uint32_t thread_idx = omp_get_thread_num();

        if(threads_doc_paths[thread_idx].size() != 0 )
        {
            for(auto& doc_path : threads_doc_paths[thread_idx])
            {
                // Load Doc
                std::string doc_str;
                LoadDoc(&doc_str, &doc_path);

                // Filter doc
                std::string doc_filtered;
                doc_filtered.reserve(sizeof(doc_str));
                filterDoc(&doc_str, &doc_filtered);

                // Analyse Doc
                if(bigram_word)
                bigramWordAnalyseChunk(&doc_filtered, &thread_bigram_word_occurrences[thread_idx]);
                if(trigram_word)
                trigramWordAnalyseChunk(&doc_filtered, &thread_trigram_word_occurrences[thread_idx]);
                if(bigram_char)
                bigramCharAnalyseChunk(&doc_filtered, &thread_bigram_char_occurrences[thread_idx]);
                if(trigram_char)
                trigramCharAnalyseChunk(&doc_filtered, &thread_trigram_char_occurrences[thread_idx]);
            
            }
        }

        #pragma omp barrier
        // Merge partial unordered_maps
        // Assume at least 4 thread TODO: check

        for(uint32_t partial_map_idx=0; partial_map_idx < max_threads; partial_map_idx++)
        {
            if(thread_idx == 0 && bigram_word)
            {
                // bigram word merge
                for(auto& iter_first_word : thread_bigram_word_occurrences[partial_map_idx])
                {
                    for(auto& iter_second_word : iter_first_word.second)
                    {
                        total_bigram_word_occurrences[iter_first_word.first][iter_second_word.first] += iter_second_word.second;
                    }
                }
            }
            else if(thread_idx == 1 && trigram_word)
            {
                // trigram word merge
                for(auto& iter_first_word : thread_trigram_word_occurrences[partial_map_idx])
                {
                    for(auto& iter_second_word : iter_first_word.second)
                    {
                        for(auto& iter_third_word : iter_second_word.second)
                        {
                            total_trigram_word_occurrences[iter_first_word.first][iter_second_word.first][iter_third_word.first] += iter_third_word.second;
                        }
                    }
                }
            }
            else if(thread_idx == 2 && bigram_char)
            {
                // bigram char merge
                for(auto& iter_char : thread_bigram_char_occurrences[partial_map_idx])
                {
                    total_bigram_char_occurrences[iter_char.first] += iter_char.second;
                }
            }
            else if(thread_idx == 3 && trigram_char)
            {
                // trigram char merge
                for(auto& iter_char : thread_trigram_char_occurrences[partial_map_idx])
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
        if(bigram_word)
        analysisToJsonFile("./../output/cpp_version/bigramWordParV2.json", &total_bigram_word_occurrences);
        if(trigram_word)
        analysisToJsonFile("./../output/cpp_version/trigramWordParV2.json", &total_trigram_word_occurrences);
        if(bigram_char)
        analysisToJsonFile("./../output/cpp_version/bigramCharParV2.json", &total_bigram_char_occurrences);
        if(trigram_char)
        analysisToJsonFile("./../output/cpp_version/trigramCharParV2.json", &total_trigram_char_occurrences);
    }

}

void LoadAndAnalysisParV3(std::string text_data_path, int32_t max_doc_num=-1, bool serialize_to_json=false,
                            bool bigram_char=false, bool trigram_char=false, bool bigram_word=false, bool trigram_word=false)
{
    // Save paths in a vector
    std::vector<fs::path> doc_paths;
    LoadPaths(text_data_path, &doc_paths, max_doc_num);
    
    
    // Sort the vector based on file sizes (to distribute them equally)
    std::sort(doc_paths.begin(), doc_paths.end(), [](const fs::path& path1, const fs::path& path2) {
        return fs::file_size(path1) < fs::file_size(path2);
    });

    
    uint32_t max_threads = omp_get_max_threads();

    // Split doc_path based on max threads
    std::vector<std::vector<fs::path>> threads_doc_paths(max_threads);
    uint32_t thread_idx = 0;
    for(auto doc_path : doc_paths)
    {
        threads_doc_paths[thread_idx % max_threads].push_back(doc_path);
        thread_idx++;
    }

    std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>> total_bigram_word_occurrences;
    std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>> total_trigram_word_occurrences;
    std::unordered_map<std::string, uint32_t> total_bigram_char_occurrences;
    std::unordered_map<std::string, uint32_t> total_trigram_char_occurrences;

    #pragma omp parallel shared(threads_doc_paths, total_bigram_word_occurrences, total_trigram_word_occurrences, total_bigram_char_occurrences, total_trigram_char_occurrences)
    {
        uint32_t thread_idx = omp_get_thread_num();

        if(threads_doc_paths[thread_idx].size() != 0 )
        {
            for(auto doc_path : threads_doc_paths[thread_idx])
            {
                // Load Doc
                std::string doc_str;
                LoadDoc(&doc_str, &doc_path);

                // Filter doc
                std::string doc_filtered;
                doc_filtered.reserve(sizeof(doc_str));
                filterDoc(&doc_str, &doc_filtered);

                #pragma omp critical
                {
                    // Analyse chunk
                    if(bigram_word)
                    bigramWordAnalyseChunk(&doc_filtered, &total_bigram_word_occurrences);
                    if(trigram_word)
                    trigramWordAnalyseChunk(&doc_filtered, &total_trigram_word_occurrences);
                    if(bigram_char)
                    bigramCharAnalyseChunk(&doc_filtered, &total_bigram_char_occurrences);
                    if(trigram_char)
                    trigramCharAnalyseChunk(&doc_filtered, &total_trigram_char_occurrences);
                }
            }
        }
    }

    // Serialize to Json
    if(serialize_to_json)
    {
        if(bigram_word)
        analysisToJsonFile("./../output/cpp_version/bigramWordParV2.json", &total_bigram_word_occurrences);
        if(trigram_word)
        analysisToJsonFile("./../output/cpp_version/trigramWordParV2.json", &total_trigram_word_occurrences);
        if(bigram_char)
        analysisToJsonFile("./../output/cpp_version/bigramCharParV2.json", &total_bigram_char_occurrences);
        if(trigram_char)
        analysisToJsonFile("./../output/cpp_version/trigramCharParV2.json", &total_trigram_char_occurrences);
    }

}

void Benchmark(benchmark_config config)
{
    /*
        benchmark_config : first number of ebook, second number of thread
    */

    // Benchmark performs only character analysis due to available hardware resources (massive Ram usage to store word's maps as documents increase)
    // This way it can test more documents

    
    benchmarks_data benchmarks;
    uint32_t main_benchmarks_number = config.ebooks_num.size();

    for(uint32_t main_benchmarks_idx=0; main_benchmarks_idx< main_benchmarks_number; main_benchmarks_idx++)
    {
        std::vector<double> elapsed_seq, elapsed_par, elapsed_parV2;
        double start_time=0, end_time=0;

        benchmarks.ebook_num.push_back(config.ebooks_num[main_benchmarks_idx]);
        benchmarks.threads_num.push_back(config.threads_num[main_benchmarks_idx]);

        omp_set_num_threads(config.threads_num[main_benchmarks_idx]);

        for(uint32_t reliability_idx=0;reliability_idx<config.iter_for_reliability;reliability_idx++)
        {
            if(config.seq_analisys[main_benchmarks_idx])
            {
                std::cout << "Seq_Analysis    : [ " << reliability_idx + 1 << " / " << config.iter_for_reliability << " ] of [ " 
                                             << main_benchmarks_idx + 1 << " / " << main_benchmarks_number << " ]" << std::flush;
                std::cout << "\r";
                start_time = omp_get_wtime();
                LoadAndAnalysisSeq("./../text_data", config.ebooks_num[main_benchmarks_idx], false, true);
                end_time = omp_get_wtime();
                elapsed_seq.push_back(end_time-start_time);
            }
            
            if(config.par_analysis[main_benchmarks_idx])
            {
                std::cout << "Par_Analysis    : [ " << reliability_idx + 1 << " / " << config.iter_for_reliability << " ] of [ "
                                                << main_benchmarks_idx + 1 << " / " << main_benchmarks_number << " ]" << std::flush;
                std::cout << "\r";
                start_time = omp_get_wtime();
                LoadAndAnalysisPar("./../text_data", config.ebooks_num[main_benchmarks_idx], false, true);
                end_time = omp_get_wtime();
                elapsed_par.push_back(end_time-start_time);
            }

            if(config.par_analysis_V2[main_benchmarks_idx])
            {
                std::cout << "Par_Analysis_V2 : [ " << reliability_idx + 1 << " / " << config.iter_for_reliability << " ] of [ "
                                                << main_benchmarks_idx + 1 << " / " << main_benchmarks_number << " ]" << std::flush;
                std::cout << "\r";
                start_time = omp_get_wtime();
                LoadAndAnalysisParV2("./../text_data", config.ebooks_num[main_benchmarks_idx], false, true);
                end_time = omp_get_wtime();
                elapsed_parV2.push_back(end_time-start_time);
            }
        }
        
        benchmarks.elapsed_seq.push_back(elapsed_seq);
        benchmarks.elapsed_par.push_back(elapsed_par);
        benchmarks.elapsed_parV2.push_back(elapsed_parV2);
    }
    
    benchmarks.ToJsonFile(config.json_output_path);

}

int main() 
{

    benchmark_config threads_config = {
        "./../output/cpp_version/benchmarks_threads.json",
        5,
        {500, 500, 500, 500, 500, 500, 500, 500, 500, 500},
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        {true, false, false, false, false, false, false, false, false, false},
        {true, true, true, true, true, true, true, true, true, true},
        {true, true, true, true, true, true, true, true, true, true}
    };

    benchmark_config ebooks_config = {
        "./../output/cpp_version/benchmarks_ebooks.json",
        5,
        {10, 100, 200, 500, 1000},
        {4, 4, 4, 4, 4},
        {true, true, true, true, true},
        {true, true, true, true, true},
        {true, true, true, true, true}
    };

    benchmark_config test_config = {
        "./../output/cpp_version/benchmarks_test.json",
        5,
        {1000},
        {4},
        {true},
        {true},
        {true}
    };

    Benchmark(threads_config);


    // double start_time=0, end_time=0;
    // std::cout << "Seq" << "\n";
    // start_time = omp_get_wtime();
    // LoadAndAnalysisSeq("./../debug_text_data", 1, true, true, true, true, true);
    // end_time = omp_get_wtime();
    // std::cout << "time :" << end_time -start_time << "\n";

    // std::cout << "Par" << "\n";
    // start_time = omp_get_wtime();
    // LoadAndAnalysisPar("./../debug_text_data", 1, true, true, true, true, true);
    // end_time = omp_get_wtime();
    // std::cout << "time :" << end_time -start_time << "\n";

    // std::cout << "ParV2" << "\n";
    // start_time = omp_get_wtime();
    // LoadAndAnalysisParV2("./../debug_text_data", 1, true, true, true, true, true);
    // end_time = omp_get_wtime();
    // std::cout << "time :" << end_time -start_time << "\n";

    // std::cout << "ParV3" << "\n";
    // start_time = omp_get_wtime();
    // LoadAndAnalysisParV3("./../text_data", 1000, false, true, false, false, false);
    // end_time = omp_get_wtime();
    // std::cout << "time :" << end_time -start_time << "\n";
}