#include "virtual_memory_data.hpp"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <cmath>

using namespace std;
using boost::lexical_cast;

namespace al = boost::algorithm;
namespace po = boost::program_options;

typedef struct
{
    uint64_t pid;
    int32_t label;
    uint64_t time;
    double played;
    double length;
}ComponentData;

typedef struct UserHistory
{
    double average;
    double length;
    vector<ComponentData> history;
    UserHistory()
    {
        average = 0.0;
        length = 0.0;
    }
}UserHistory;

int main(int c, char** v)
{
    string input_path, output_path;
    po::options_description desc("make binary file from user history :");
    desc.add_options()
        ("help,h", "show messages.")
        ("input,i", po::value<string>(&input_path), "input format: uid pid <history>")
        ("output,o", po::value<string>(&output_path), "output as binary<index, block>");
    po::variables_map vm;
    po::store(po::parse_command_line(c, v, desc), vm);
    po::notify(vm);
    if(vm.count("help") || c < 3 || input_path.size() == 0 || output_path.size() == 0)
    {
        cerr << desc << endl;
        return 0;
    }
    ifstream fin(input_path.c_str());
    if(!fin.is_open())
    {
        cerr << input_path << " can not be opened."<<endl;
        return 0; 
    }

    kaijiang_api::VirtualMemoryDataWriter<ComponentData> writer;
    if(!writer.Open(output_path.c_str()))
        return 0;

    std::string str;
    std::vector<std::string> splites;
    ComponentData data;
    map<uint64_t, UserHistory> user_history;
    while(getline(fin, str))
    {
        al::split(splites, str, al::is_any_of(" "));
        if(splites.size() < 4)
            continue;

        try
        {
            uint64_t uid = lexical_cast<uint64_t>(splites[0]);
            data.pid = lexical_cast<uint64_t>(splites[1]);
            data.label = lexical_cast<int32_t>(splites[2]);
            data.time = lexical_cast<uint64_t>(splites[3]);
            data.played = 0.0;
            if(splites.size() > 4)
            {
               al::trim(splites[4]);
               if(splites[4].size() > 0)
                   data.played = lexical_cast<double>(splites[4]);
            }
            data.length = 0.0;
            if(splites.size() > 5)
            {
                al::trim(splites[5]);
                if(splites[5].size() > 0)
                    data.length = lexical_cast<double>(splites[5]);
            }

            user_history[uid].history.push_back(data);
            user_history[uid].average += data.played;
            user_history[uid].length += data.length;
        }
        catch(...)
        {
            continue;
        }
    }

    for(auto &u : user_history)
    {
        writer.SwitchIndex(u.first);

        data.pid = 0;
        data.label = -1;
        data.time = 0;
        data.played = u.second.average / u.second.length;
        data.length = 0.0;
        writer.Write(data);

        for(auto &d : u.second.history)
        {
            writer.Write(d);
        }
    }
    writer.Close();
    fin.close();

    return 0;
}
