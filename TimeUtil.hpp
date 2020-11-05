#ifndef TIME_UTIL_HPP
#define TIME_UTIL_HPP

#include <chrono> 
#include <ctime>
#include <string>

class TimeUtil{
    public:
    TimeUtil(){

    }
    ~TimeUtil(){

    }

    std::string Now(){
        std::string now;
        time_t curr_time;
	    curr_time = time(NULL);
	    tm *tm_local = localtime(&curr_time);
        now = std::to_string(tm_local->tm_year - 100) + "/";
        now.append(std::to_string(tm_local->tm_mon + 1) + "/");
        now.append(std::to_string(tm_local->tm_mday) + " ");
        now.append(std::to_string(tm_local->tm_hour) + ":");
        now.append(std::to_string(tm_local->tm_min)  + ":");
        now.append(std::to_string(tm_local->tm_sec));         
        return now;
    }
    
    std::string Reset() {
        std::string Reset("\033[0m");
        return Reset;
    }
    std::string Red() {
        std::string Red("\033[0;31m"); 
        return Red;
     }
    std::string Green() {
        std::string Green("\033[0;32m");
        return Green;
    }
    std::string Yellow() {
        std::string Yellow("\033[0;33m");
        return Yellow;
    }
};

#endif /* TIME_UTIL_HPP */