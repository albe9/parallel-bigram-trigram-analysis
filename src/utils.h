#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <regex>

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
                json_file << "\n";
            }
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

void filterDoc(std::string* doc_str, std::string* doc_filtered)
{
    std::regex base_filter("[^a-zA-Z \n.]");
    std::regex advanced_filter("(\n|\r|\\. \\. \\.)");
    std::regex period_filter("\\. ");

    *doc_filtered = std::regex_replace(*doc_str, base_filter, "");
    *doc_filtered = std::regex_replace(*doc_filtered, advanced_filter, " ");
    *doc_filtered = std::regex_replace(*doc_filtered, period_filter, " - ");

    std::transform(doc_filtered->begin(), doc_filtered->end(), doc_filtered->begin(), ::tolower);

}

void bigramWordAnalyseChunk(std::string* chunk_ptr, std::unordered_map<std::string, std::unordered_map<std::string, uint32_t>>*  bigram_occurrences)
{
    std::stringstream ss(*chunk_ptr);
    
    std::string current_word="", first_word="";

    while (std::getline(ss, current_word, ' '))
    {
        if(current_word != "")
        {
            if(current_word == "-")
            {
                first_word = "";
            }
            else if(first_word == "")
            {
                first_word = current_word;
            }
            else
            {
                (*bigram_occurrences)[first_word][current_word]++;
                first_word = current_word;
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
        if(current_word != "")
        {
            if(current_word == "-")
            {
                first_word = "";
                second_word = "";
            }
            else if(first_word == "")
            {
                first_word = current_word;
            }
            else if(second_word == "")
            {
                second_word = current_word;
            }
            else
            {
                (*trigram_occurrences)[first_word][second_word][current_word]++;
                first_word = second_word;
                second_word = current_word;
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
        if(current_word != "" && current_word != "-")
        {
            std::string bigram;
            for(int char_idx=0; char_idx<current_word.length(); char_idx++)
            {
                if(char_idx == 0)
                {
                    bigram = current_word[char_idx];
                }
                else
                {
                    bigram += current_word[char_idx];
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
        if(current_word != "" && current_word != "-")
        {
            std::string trigram;
            for(int char_idx=0; char_idx<current_word.length(); char_idx++)
            {
                if(char_idx == 0)
                {
                    trigram = current_word[char_idx];
                }
                else if(char_idx == 1){
                    trigram += current_word[char_idx];
                }
                else
                {
                    trigram += current_word[char_idx];
                    (*trigram_occurrences)[trigram]++;
                    trigram = trigram.substr(1,3);
                }
            }
        }
    }
}

void LoadPaths(std::string text_data_path, std::vector<fs::path>* doc_paths, int max_doc_num)
{
    // Save path in a vector
    
    if (std::filesystem::exists(text_data_path) && std::filesystem::is_directory(text_data_path))
    {
        for (const auto& entry : std::filesystem::directory_iterator(text_data_path)) 
        {
            if(doc_paths->size() >= max_doc_num)
            {
                break;
            }
            else if(std::filesystem::is_regular_file(entry))
            {
                doc_paths->push_back(entry.path());
            }
        }
    } 
    else 
    {
        std::cerr << "The specified directory does not exist or is not a directory." << std::endl;
    }
}

void LoadDoc(std::string* doc_str, fs::path* doc_path)
{
    doc_str->resize(fs::file_size(*doc_path));
    doc_str->clear();

    std::ifstream doc_input(*doc_path, std::ios::in);
    if(!doc_input.is_open())
    {
        std::cout << *doc_path << " not opened" << std::endl;
        // TODO handle opening error
    }

    // Reading and preprocessing doc one line at a time
    std::string line;
    while(std::getline(doc_input, line))
    {

        // if (line.back() == '\n')
        // {
        //     line.pop_back();
        // }    
        // else if (line.back() == '\r')
        // {
        //     line.pop_back();
        //     line.push_back(' ');
        // }
        line += "\n";
        (*doc_str) += line;
    }
}
