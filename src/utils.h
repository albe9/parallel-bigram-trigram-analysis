#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>


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

        if (line.back() == '\n')
        {
            line.pop_back();
        }    
        else if (line.back() == '\r')
        {
            line.pop_back();
            line.push_back(' ');
        }
        (*doc_str) += line;
    }
}
