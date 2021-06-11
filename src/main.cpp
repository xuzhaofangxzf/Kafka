#include <iostream>
#include <vector>
#include <string>
#include <signal.h>
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


map<string, KafkaProducer*> KafkaClients; //<topic, KafkaProducer>
bool g_isStop = false;

void sigStop(int signo)
{
    LOG_ERROR("receive signal %d, exit !", signo);
    g_isStop = true;
}
std::string getKafkaInfo(const std::string &strIn, const char delim, int loc = 3)
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
    std::string KafkaInfo;
    std::string topic;
    std::string broker_list;
    std::vector<std::string> KafkaTokens;

    KafkaProducer* dataProducer = new KafkaProducer("10.173.194.22:39092", "common-spider-data", 0);
    KafkaProducer* logProducer = new KafkaProducer("10.173.194.22:39092", "common-spider-epoch", 0);
    KafkaClients["common_spider_data"] = dataProducer;
    KafkaClients["common_spider_epoch"] = logProducer;
    size_t pos = 0;
    // auto start = steady_clock::now();
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

        if ((pos = inputStream.find("output\tscribe")) != std::string::npos) {
            std::string strTmp = inputStream.substr(pos); //获取从output\tscribe到最后的子串
            KafkaInfo = getKafkaInfo(strTmp, '\t');
            if(KafkaInfo == "")
            {
                continue;
            }
            split(KafkaInfo, '|', KafkaTokens);
            if (KafkaTokens.size() < 2) {
                continue;
            }
            topic = KafkaTokens[0];
            broker_list = KafkaTokens[1];
            LOG_INFO("file:%s\tline:%d\ttopic:%s\tbroker_list:%s", __FILE__, __LINE__, topic.c_str(), broker_list.c_str());
            if (topic.empty() || broker_list.empty()) {
                continue;
            }
            try
            {
                inputStream = inputStream.substr(inputStream.find(KafkaInfo) + KafkaInfo.size());
            }
            catch(...)
            {
                LOG_ERROR("file:%s\tline:%d\tinputdata error\t%s",
                __FILE__,
                __LINE__,
                inputStream.c_str());
                continue;
            }
            LOG_INFO("file:%s\tline:%d\tinputdata:\t%s",
                __FILE__,
                __LINE__,
                inputStream.c_str());
            if (KafkaClients.find(topic) != KafkaClients.end()) {
                KafkaClients[topic]->pushMessage(inputStream, "");
                continue;
            }
            KafkaProducer* Producer = new KafkaProducer(broker_list, topic, 0);
            KafkaClients[topic] = Producer;
            Producer->pushMessage(inputStream, "");
        }
    }
    // auto end = steady_clock::now();
    // auto duration = duration_cast<microseconds>(end - start);
    // cout <<  "reg1:"  << double(duration.count()) * microseconds::period::num / microseconds::period::den << "s" << endl;
    RdKafka::wait_destroyed(5000);
    // delete dataProducer;
    // delete logProducer;
    for (const auto element : KafkaClients)
    {
        delete element.second;
    }
    return 0;
}