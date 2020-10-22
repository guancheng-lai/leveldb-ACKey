//
// Created by Guancheng Lai on 10/21/20.
//

#ifndef LEVELDB_SIMPLEMETRICS_HPP
#define LEVELDB_SIMPLEMETRICS_HPP

#include <fstream>
#include <unordered_map>
#include <iostream>
#include <chrono>
#include <ctime>

class SimpleMetrics {
public:
  static SimpleMetrics& GetMetrics() {
    static SimpleMetrics instance;
    return instance;
  }

  void AddMessage(const std::string& title, const std::string& value) {
    message[title] = value;
  }

  void AddProperty(const std::string& p, const std::string& activity)  {
    info[p][activity]++;
  }
private:
  SimpleMetrics(SimpleMetrics const&) = delete;

  void operator=(SimpleMetrics const&) = delete;

  SimpleMetrics() {
    start = std::chrono::system_clock::now();
    fs = std::fstream("metrics.txt", std::fstream::app);
  }

  ~SimpleMetrics() {
    end = std::chrono::system_clock::now();

    std::chrono::duration<float> elapsed_seconds = end - start;
    std::time_t end_time = std::chrono::system_clock::to_time_t(end);

    fs << "\n---------------------------------------------\n";
    fs << "Finished computation at " << std::ctime(&end_time);
    fs << "Elapsed time: " << elapsed_seconds.count() << "s\n";

    for (const auto& it : message) {
      fs << it.first << " = " << it.second << std::endl;
    }

    for (const auto& it : info) {
      uint64_t total = 0;
      for (const auto& propertyIt : it.second) {
         total += propertyIt.second;
      }
      fs << "--------------" << it.first << "--------------\n";
      for (const auto& propertyIt : it.second) {
        fs << propertyIt.first << " rate = " << propertyIt.second / (double) total << std::endl;
      }
      fs << "--------------" << it.first << "--------------\n\n";
    }

    fs << "---------------------------------------------" << std::endl;
  }

  std::fstream fs;
  std::unordered_map<std::string, std::string> message;
  std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> info;
  std::chrono::system_clock::time_point start, end;
};

#endif //LEVELDB_SIMPLEMETRICS_HPP
