#include <iostream>
#include <vector>
#include <string>
#include <signal.h>
#include <regex>
#include <map>
#include <string.h>
#include "KafkaProducer.h"
#include "KafkaProducerLog.h"
using namespace std;
#define DEFALUT_LOG "../log/kafka.log"

map<string,string> defaultKafka = {
    {"common-spider-data", "10.173.194.22:39092"},
    {"common-spider-epoch","10.173.194.22:39092"}};
map<string, KafkaProducer*> specialClinet; //<topic, KafkaProducer>
bool g_isStop = false;

void sigStop(int signo)
{
    log (LOG_WARNING, "receive signal %d, exit !", signo);
    g_isStop = true;
}

std::string getCategory(const std::string &strIn, const char delim, int loc = 3)
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
int main()
{
    // // 创建Producer
    // KafkaProducer producer("10.173.194.22:39092	", "special-spider-epoch", 0);
    // for(int i = 0; i < 10; i++)
    // {
    //     char msg[64] = {0};
    //     sprintf(msg, "%s%4d", "Hello RdKafka", i);
    //     // 生产消息
    //     char key[8] = {0};
    //     sprintf(key, "%d", i);
    //     producer.pushMessage(msg, key);
    // }
    // RdKafka::wait_destroyed(5000);
    log_init(DEFALUT_LOG, LOG_YEAR_DAY_HOUR, LOG_NOTICE);
    std::string inputStream;
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = sigStop;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    regex topicPattern("\"opts\":.*?topic:(.*?);");
    regex BrokerlistPattern("\"opts\":.*?broken-list:(.*?);");
    smatch result;
    string topic;
    string brokerlist;
    std::string strCatogory;
    KafkaProducer* dataProducer = new KafkaProducer("10.173.194.22:39092", "common-spider-data", 0);
    KafkaProducer* logProducer = new KafkaProducer("10.173.194.22:39092", "common-spider-epoch", 0);


    size_t pos = 0;
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

        if ((pos = inputStream.find("output\tscribe")) != std::string::npos)
        {
            std::string strTmp = inputStream.substr(pos); //获取从output\tscribe到最后的子串
            strCatogory = getCategory(strTmp, '\t');
            if(strCatogory == "")
            {
                continue;
            }
            try
            {
                inputStream = inputStream.substr(inputStream.find(strCatogory) + strCatogory.size() + sizeof("\t"));
            }
            catch(...)
            {
                log (LOG_WARNING, "file:%s\tline:%d\tinputdata error, category=%s",
                __FILE__,
                __LINE__,
                strCatogory.c_str());
                continue;
            }
            //先判断是否有topic和brokerlist
            if (regex_search(inputStream, result, topicPattern))
            {
                topic = result.str(1);
                if (regex_search(inputStream, result, BrokerlistPattern))
                {
                    brokerlist = result.str(1);
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
                    continue; //已经处理过数据了，不需要再处理了
                }
            }
            if (strCatogory == "common_spider_log")
            {
                logProducer->pushMessage(inputStream, "");
            }
            else if(strCatogory == "common_spider_data")
            {
                dataProducer->pushMessage(inputStream, "");
            }
        }
        else
        {
            log (LOG_WARNING, "==================error data! =====================");
            // std::cout << "==================error data! =====================" << std::endl;
        }

    }
    RdKafka::wait_destroyed(5000);
    delete dataProducer;
    delete logProducer;
    for (const auto element : specialClinet)
    {
        delete element.second;
    }
    return 0;
}