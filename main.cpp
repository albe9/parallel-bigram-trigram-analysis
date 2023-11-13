#include "matplotlibcpp.h"
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>

#define CHUNK_SIZE 2000000  //max size in bytes

namespace plt = matplotlibcpp;
namespace fs = std::filesystem;

std::string readDocument(std::string doc_path_str)
{

    std::filesystem::path doc_path(doc_path_str);
    ssize_t doc_size = std::filesystem::file_size(doc_path);

    if(doc_size > CHUNK_SIZE)
    {
        std::cout << "The size of " << doc_path_str << " is " << doc_size / 1000000 << " mB. Need to be divided in chunk\n";
        // TODO divide in chunk
        return("");
    }

    

    std::ifstream doc_input(doc_path, std::ios::in);
    if(!doc_input.is_open())
    {
        std::cout << doc_path << " not opened" << std::endl;
        return "";
    }

    std::string doc_str;
    doc_str.reserve(doc_size);
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

    return doc_str;
}

// Bigram word to json
void analysisToJsonFile(std::string json_path, std::map<std::string, std::map<std::string, uint32_t>> *bigram_occurences){
    std::ofstream json_file(json_path, std::ios::out);

    json_file << "{\n";

    for(auto iter = bigram_occurences->begin(); iter != bigram_occurences->end(); iter++)
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
        if(std::next(iter) != bigram_occurences->end())
        {
            json_file << ",";
        }
        json_file << "\n";
    }

    json_file << "}";

}
// Trigram word to json
void analysisToJsonFile(std::string json_path, std::map<std::string, std::map<std::string, std::map<std::string, uint32_t>>> *trigram_occurences){
    std::ofstream json_file(json_path, std::ios::out);

    json_file << "{\n";

    for(auto iter = trigram_occurences->begin(); iter != trigram_occurences->end(); iter++)
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
        if(std::next(iter) != trigram_occurences->end())
        {
            json_file << ",";
        }
        json_file << "\n";
    }

    json_file << "}";

}
// Bigram or Trigram to json
void analysisToJsonFile(std::string json_path, std::map<std::string, uint32_t>* occurences){
    std::ofstream json_file(json_path, std::ios::out);

    json_file << "{\n";

    for(auto iter = occurences->begin(); iter != occurences->end(); iter++)
    {
        json_file << "\t\"" << iter->first << "\" : "<< iter->second;
        
        if(std::next(iter) != occurences->end())
        {
            json_file << ",";
        }
        json_file << "\n";
    }

    json_file << "}";

}

inline std::string filterString(std::string str_to_filter)
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
    }
    
    return str_filtered;

}

void bigramWordAnalyseChunk(std::string* chunk_ptr, std::map<std::string, std::map<std::string, uint32_t>>*  bigram_occurences)
{
    std::stringstream ss(*chunk_ptr);
    
    std::string current_word="", first_word="";
    while (std::getline(ss, current_word, ' '))
    {
        std::string word_filtered = filterString(current_word);

        if(word_filtered != "")
        {
            if(first_word == "")
            {
                first_word = word_filtered;
            }
            else
            {
                (*bigram_occurences)[first_word][word_filtered]++;
                first_word = word_filtered;
            }
        }
    }
}

void trigramWordAnalyseChunk(std::string* chunk_ptr, std::map<std::string, std::map<std::string, std::map<std::string, uint32_t>>>*  trigram_occurences)
{
    std::stringstream ss(*chunk_ptr);
    
    std::string current_word="", first_word="", second_word="";
    while (std::getline(ss, current_word, ' '))
    {
        std::string word_filtered = filterString(current_word);

        if(word_filtered != "")
        {
            if(first_word == "")
            {
                first_word = word_filtered;
            }
            else if(second_word == "")
            {
                second_word = word_filtered;
            }
            else
            {
                (*trigram_occurences)[first_word][second_word][word_filtered]++;
                first_word = second_word;
                second_word = word_filtered;
            }
        }
    }
}

void bigramCharAnalyseChunk(std::string* chunk_ptr, std::map<std::string, uint32_t>* bigram_occurences)
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
                    (*bigram_occurences)[bigram]++;
                    bigram = bigram[1];
                }
            }
        }
    }
}

void trigramCharAnalyseChunk(std::string* chunk_ptr, std::map<std::string, uint32_t>* trigram_occurences)
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
                    (*trigram_occurences)[trigram]++;
                    trigram = trigram.substr(1,3);
                }
            }
        }
    }
}

void LoadAndAnalysisSeq(std::string text_data_path)
{


    // Save path in a vector
    std::vector<fs::path> doc_paths;

    if (std::filesystem::exists(text_data_path) && std::filesystem::is_directory(text_data_path))
    {
        for (const auto& entry : std::filesystem::directory_iterator(text_data_path)) 
        {
            if (std::filesystem::is_regular_file(entry))
            {
                doc_paths.push_back(entry.path());
            }
        }
    } 
    else 
    {
        std::cerr << "The specified directory does not exist or is not a directory." << std::endl;
    }
    
    // Iter through file and save data in chunk 
    std::streampos last_doc_pos = -1;
    auto last_doc_path = doc_paths.begin();
    bool docs_ended = false;

    std::map<std::string, std::map<std::string, uint32_t>> bigram_word_occurences;
    std::map<std::string, std::map<std::string, std::map<std::string, uint32_t>>> trigram_word_occurences;
    std::map<std::string, uint32_t> bigram_char_occurences;
    std::map<std::string, uint32_t> trigram_char_occurences;

    while(!docs_ended)
    {
        std::string chunk_str;
        chunk_str.reserve(CHUNK_SIZE);
        bool chunk_full = false;

        while(!chunk_full)
        {
            // for loop starting from last doc path (initialized with doc_path.begin())
            for(auto iter_doc_path=last_doc_path; iter_doc_path != doc_paths.end(); iter_doc_path++)
            {

                std::ifstream doc_input(*iter_doc_path, std::ios::in);
                if(!doc_input.is_open())
                {
                    std::cout << *iter_doc_path << " not opened" << std::endl;
                    // TODO handle opening error
                }

                if(last_doc_pos != -1)
                {
                    doc_input.seekg(last_doc_pos);
                }

                std::string line;

                while(std::getline(doc_input, line))
                {

                    if(chunk_str.length() >= CHUNK_SIZE)
                    {
                        chunk_full = true;
                        last_doc_pos = doc_input.tellg() - (std::streampos)line.length();
                        last_doc_path = iter_doc_path;
                        break;
                    }
                    else
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
                        chunk_str += line;
                    }
                }

                // Last document, end loop even if chunk not full
                if(std::next(iter_doc_path) == doc_paths.end())
                {
                    chunk_full = true;
                    docs_ended = true;
                }
                else if(chunk_full)
                {
                    break;
                }
            }
        }

        // Analyse chunk
        bigramWordAnalyseChunk(&chunk_str, &bigram_word_occurences);
        trigramWordAnalyseChunk(&chunk_str, &trigram_word_occurences);
        bigramCharAnalyseChunk(&chunk_str, &bigram_char_occurences);
        trigramCharAnalyseChunk(&chunk_str, &trigram_char_occurences);
    }

    // Serialize to Json
    analysisToJsonFile("./../../output/bigramWord.json", &bigram_word_occurences);
    analysisToJsonFile("./../../output/trigramWord.json", &trigram_word_occurences);
    analysisToJsonFile("./../../output/bigramChar.json", &bigram_char_occurences);
    analysisToJsonFile("./../../output/trigramChar.json", &trigram_char_occurences);
    


}

void LoadAndAnalysisPar(std::string text_data_path)
{
    
}

int main() 
{
    LoadAndAnalysisSeq("./../../text_data");
}