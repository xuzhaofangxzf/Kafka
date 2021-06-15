#include <iostream>
#include <vector>
#include <string>
#include <signal.h>
// #include <regex>
#include <sstream>
#include <map>
#include <string.h>
#include <chrono>
#include "KafkaProducer.h"
#include "rlog.h"
#include "CJsonObject.h"
using namespace std;
using namespace chrono;
#define DEFALUT_LOG "../log/"

// map<string,string> defaultKafka = {
//     {"common-spider-data", "10.173.194.22:39092"},
//     {"common-spider-epoch","10.173.194.22:39092"}};
map<string, KafkaProducer*> specialClinet; //<topic, KafkaProducer>
bool g_isStop = false;

void sigStop(int signo)
{
    LOG_ERROR("receive signal %d, exit !", signo);
    g_isStop = true;
}
#if 0
std::string getKeyValue(const std::string &strIn, const char delim, int loc = 1)
{
    if (strIn.empty())
    {
        return strIn;
    }
    std::stringstream input(strIn);   //读取str到字符串流中
    std::string res;
    //使用getline函数从字符串流中读取,遇到分隔符时停止,和从cin中读取类似
    //注意,getline默认是可以读取空格的
    while(std::getline(input, res, delim))
    {
        loc--;
        if (loc == 0)
        {
            break;
        }
    }
    return res;
}
#endif
void split(const std::string& s, char delimiter, std::vector<std::string>& tokens)
{
   std::string token;
   std::istringstream tokenStream(s);
   while (std::getline(tokenStream, token, delimiter))
   {
      tokens.push_back(token);
   }
   return;
}

int main()
{
    LOG_INIT(DEFALUT_LOG, "kafka", INFO);
    std::string inputStream;
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = sigStop;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    // regex topicPattern("\"opts\":.*topic:(.*?);");
    // regex BrokerlistPattern("\"opts\":.*broker_list:(.*?);");
    // regex topicPattern("topic:(.*?);");
    // regex BrokerlistPattern("broker_list:(.*?);");
    // smatch result;


    KafkaProducer* dataProducer = new KafkaProducer("10.173.194.22:39092", "spider_mobile_data", 0);
    KafkaProducer* logProducer = new KafkaProducer("10.173.194.22:39092", "common-spider-epoch", 0);
    //KafkaProducer* MobledataProducer = new KafkaProducer("10.173.194.22:39092", "spider_mobile_data", 0);

    size_t pos = 0;
    auto start = steady_clock::now();
    while(!g_isStop && getline(std::cin, inputStream))
    {
        if (inputStream.empty())
        {
            continue;
        }
        if(inputStream == "\n" || inputStream == "\r\n" || inputStream == "\r") //空行
        {
            continue;
        }
        if(inputStream[0] == '\0') //空行
        {
            continue;
        }

        if ((pos = inputStream.find("output\tscribe\tcommon_spider_data")) != std::string::npos) {
            try
            {
                inputStream = inputStream.substr(pos + sizeof("output\tscribe\tcommon_spider_data\t") - 1);
            }
            catch(...)
            {
                LOG_ERROR("file:%s\tline:%d\tinputdata error\t%s",
                __FILE__,
                __LINE__,
                inputStream.c_str());
                continue;
            }
            neb::CJsonObject oJson(inputStream);
            std::string strTraversingKey;
            std::string strTraversingValue;
            while(oJson.GetKey(strTraversingKey))
            {
                if (strTraversingKey == "opts")
                {
                    oJson.Get(strTraversingKey, strTraversingValue);
                    break;
                }
            }
            if (strTraversingValue.empty() || strTraversingValue.find("topic") == std::string::npos || strTraversingValue.find("broken-list") == std::string::npos) {
                dataProducer->pushMessage(inputStream, "");
                continue;
            }
            string topic;
            string brokerlist;
            std::vector<std::string> tokens;
            bool isSpecal = false;
            start = steady_clock::now();
            split(strTraversingValue, ';', tokens);         
            for (int i = 0; i < tokens.size(); ++i) {
                // std::cout << "tokens_" << i  << " " << tokens[i] << std::endl;
                std::vector<std::string> token_list;
                split(tokens[i], ':', token_list);
                if (token_list.size() < 2) continue;
                if (token_list[0] == "topic") {
                    topic = token_list[1];
                }
                if (token_list.size() < 3) continue;
                if (token_list[0] == "broken-list") {
                    brokerlist = token_list[1] + ":" + token_list[2];
                }
            }
            if (!topic.empty() && !brokerlist.empty()) {
                // std::cout << "topic:" << topic  << "\t" << "broker_list:" << brokerlist << std::endl;
                
                if ((specialClinet.find(topic)) == specialClinet.end())
                {
                    KafkaProducer* producer = new KafkaProducer(brokerlist, topic, 0);
                    specialClinet[topic] = producer;
                    producer->pushMessage(inputStream, "");
                }
                else
                {
                    specialClinet[topic]->pushMessage(inputStream, "");
                }
                isSpecal = true;
                break;
            }
            if (isSpecal) {
                continue;
            }
            dataProducer->pushMessage(inputStream, "");
        }
        else if ((pos = inputStream.find("output\tscribe\tqss.zzzc-zlib-spider-deadlink-recheck-debug")) != std::string::npos) {
            try
            {
                inputStream = inputStream.substr(pos + sizeof("output\tscribe\tqss.zzzc-zlib-spider-deadlink-recheck-debug\t") - 1);
            }
            catch(...)
            {
                LOG_ERROR("file:%s\tline:%d\tinputdata error\t%s",
                __FILE__,
                __LINE__,
                inputStream.c_str());
                continue;
            }
            neb::CJsonObject oJson(inputStream);
            std::string strTraversingKey;
            std::string strTraversingValue;
            while(oJson.GetKey(strTraversingKey))
            {
                if (strTraversingKey == "opts")
                {
                    oJson.Get(strTraversingKey, strTraversingValue);
                    break;
                }
            }
            if (strTraversingValue.empty() || strTraversingValue.find("topic") == std::string::npos || strTraversingValue.find("broken-list") == std::string::npos) {
                dataProducer->pushMessage(inputStream, "");
                continue;
            }
            string topic;
            string brokerlist;
            std::vector<std::string> tokens;
            bool isSpecal = false;
            start = steady_clock::now();
            split(strTraversingValue, ';', tokens);         
            for (int i = 0; i < tokens.size(); ++i) {
                // std::cout << "tokens_" << i  << " " << tokens[i] << std::endl;
                std::vector<std::string> token_list;
                split(tokens[i], ':', token_list);
                if (token_list.size() < 2) continue;
                if (token_list[0] == "topic") {
                    topic = token_list[1];
                }
                if (token_list.size() < 3) continue;
                if (token_list[0] == "broken-list") {
                    brokerlist = token_list[1] + ":" + token_list[2];
                }
                if (!topic.empty() && !brokerlist.empty()) {
                    // std::cout << "topic:" << topic  << "\t" << "broker_list:" << brokerlist << std::endl;
                    
                    if ((specialClinet.find(topic)) == specialClinet.end())
                    {
                        KafkaProducer* producer = new KafkaProducer(brokerlist, topic, 0);
                        specialClinet[topic] = producer;
                        producer->pushMessage(inputStream, "");
                    }
                    else
                    {
                        specialClinet[topic]->pushMessage(inputStream, "");
                    }
                    isSpecal = true;
                    break;
                }
            }
            if (isSpecal) {
                continue;
            }
            dataProducer->pushMessage(inputStream, "");
        }
        else if ((pos = inputStream.find("output\tscribe\tcommon_spider_log")) != std::string::npos) {
            try
            {
                inputStream = inputStream.substr(pos + sizeof("output\tscribe\tcommon_spider_log\t") - 1);
            }
            catch(...)
            {
                LOG_ERROR("file:%s\tline:%d\tinputdata error\t%s",
                __FILE__,
                __LINE__,
                inputStream.c_str());
                continue;
            }
            logProducer->pushMessage(inputStream, "");
        }

    }

    // auto end = steady_clock::now();
    // auto duration = duration_cast<microseconds>(end - start);
    // cout <<  "reg1:"  << double(duration.count()) * microseconds::period::num / microseconds::period::den << "s" << endl;
    RdKafka::wait_destroyed(5000);
    delete dataProducer;
    delete logProducer;
    for (const auto element : specialClinet)
    {
        delete element.second;
    }
    return 0;
}