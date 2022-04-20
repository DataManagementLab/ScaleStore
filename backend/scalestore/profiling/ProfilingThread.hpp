#pragma once
#include "CounterRegistry.hpp"
#include "counters/BMCounters.hpp"
#include "counters/WorkerCounters.hpp"
#include "Tabulate.hpp"
// -------------------------------------------------------------------------------------
#include <chrono>
#include <iostream>
#include <filesystem>
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace profiling
{

struct WorkloadInfo{
   virtual std::vector<std::string> getRow() = 0;
   virtual std::vector<std::string> getHeader() = 0;
   virtual void csv(std::ofstream& file) = 0;
   virtual void csvHeader(std::ofstream& file) = 0;
};

struct ProfilingThread {
   void profile(NodeID nodeId, WorkloadInfo& wlInfo, [[maybe_unused]] storage::Buffermanager& bm)
   {
      using namespace std::chrono_literals;
      std::locale::global(std::locale("C")); // hack to restore locale which is messed up in tabulate package
      std::vector<uint64_t> workerCounterAgg(WorkerCounters::COUNT, 0);
      std::vector<uint64_t> bmCounterAgg(BMCounters::COUNT, 0);
      std::vector<double> rdmaCounterAgg(RDMACounters::COUNT, 0);
      std::unordered_map<std::string, double> cpuCountersAgg;
      // csv file
      std::ofstream csv_file;
      std::ofstream::openmode open_flags = std::ios::app;
      bool csv_initialized = std::filesystem::exists(FLAGS_csvFile);

      if (FLAGS_csv) {
         // create file and fill with headers
         csv_file.open(FLAGS_csvFile, open_flags);
      }

      auto convert_precision = [](double number) -> std::string {
         std::ios stream_state(nullptr);
         std::stringstream stream;
         stream_state.copyfmt(stream);
         stream << std::fixed << std::setprecision(2) << number;
         stream.copyfmt(stream_state);
         return stream.str();
      };

      auto convert_humanreadable = [&](double number)-> std::string {
         double hr_tx = 0;
         std::string unit = "";
         if (number >= 1e6) {
            hr_tx = number / 1000000.0;
            unit = "M";
         } else if (number >= 1e4) {
            hr_tx = number / 1000.0;
            unit = "K";
         }else{
            return convert_precision(number);
         }
         return convert_precision(hr_tx) + unit;
      };

      uint64_t seconds = 0;

      tabulate::Table::Row_t header;
      tabulate::Table::Row_t row;
      auto next = std::chrono::system_clock::now() + 1s;
      while (running) {
         seconds++;
         // -------------------------------------------------------------------------------------
         CounterRegistry::getInstance().aggregateWorkerCounters(workerCounterAgg);
         // -------------------------------------------------------------------------------------
         for (uint64_t c_i = 0; c_i < WorkerCounters::COUNT; c_i++) {
            if (WorkerCounters::workerCounterLogLevel[c_i].level > ACTIVE_LOG_LEVEL)
               continue;
            // -------------------------------------------------------------------------------------
            if(c_i == WorkerCounters::tx_p){
               header.push_back({WorkerCounters::workerCounterTranslation[c_i]});
               row.push_back(convert_humanreadable(workerCounterAgg[c_i]));
               continue;
            }
            // -------------------------------------------------------------------------------------
            if (c_i == WorkerCounters::latency && workerCounterAgg[WorkerCounters::tx_p] > 0) {
               header.push_back({WorkerCounters::workerCounterTranslation[c_i]});
               row.push_back(std::string(convert_precision(workerCounterAgg[c_i] / (double)workerCounterAgg[WorkerCounters::tx_p])));
               continue;
            }
            // -------------------------------------------------------------------------------------
            header.push_back({WorkerCounters::workerCounterTranslation[c_i]});
            row.push_back(std::string(std::to_string(workerCounterAgg[c_i])));
         }
         // -------------------------------------------------------------------------------------
         CounterRegistry::getInstance().aggregateBMCounters(bmCounterAgg);
         for (uint64_t c_i = 0; c_i < BMCounters::COUNT; c_i++) {
            if (BMCounters::bmCounterLogLevel[c_i].level > ACTIVE_LOG_LEVEL)
               continue;

            if(c_i == BMCounters::consumedPages){
               header.push_back({"pages (GB)"});
               row.push_back(std::string(convert_precision((((double)bmCounterAgg[c_i] * sizeof(scalestore::storage::Page))/ (1024.0*1024*1024)))));
               continue;
            }
            header.push_back({BMCounters::translation[c_i]});
            row.push_back(std::string(std::to_string(bmCounterAgg[c_i])));
         }
         // -------------------------------------------------------------------------------------
         auto tx_p = workerCounterAgg[WorkerCounters::tx_p];
         CounterRegistry::getInstance().aggregateCPUCounter(cpuCountersAgg);
         header.insert(header.end(), {{"inst/tx"}, {"L1-M/tx"}, {"cycl/tx"}, {"LLC-M/tx"}, {"CPU"}});
         row.insert(row.end(), { convert_humanreadable(cpuCountersAgg["instructions"]/tx_p),
                                 convert_precision(cpuCountersAgg["L1-misses"] / tx_p),
                                 convert_humanreadable(cpuCountersAgg["cycles"] / tx_p),
                                 convert_precision(cpuCountersAgg["LLC-misses"] / tx_p),
                                 convert_precision(cpuCountersAgg["CPU"])});

         // -------------------------------------------------------------------------------------
         // workload info
         auto wl_header = wlInfo.getHeader();
         header.insert(header.end(), wl_header.begin(), wl_header.end());
         auto wl_row = wlInfo.getRow();
         row.insert(row.end(), wl_row.begin(), wl_row.end());
         CounterRegistry::getInstance().aggregateRDMACounters(rdmaCounterAgg);
         for (uint64_t c_i = 0; c_i < RDMACounters::COUNT; c_i++) {
            header.push_back(RDMACounters::translation[c_i]);
            row.push_back(convert_precision(rdmaCounterAgg[c_i]));
         }
         // -------------------------------------------------------------------------------------
         // table
         {
            tabulate::Table table;
            table.format().width(10);
            if (seconds == 1) {
               table.add_row(header);
               table.add_row(row);
            } else {
               table.format().hide_border_top();
               table.add_row(row);
            }
            row.clear();
            header.clear();
            std::cout << table << std::endl;
         }
         // -------------------------------------------------------------------------------------
         // write to csv
         // -------------------------------------------------------------------------------------
         if (FLAGS_csv) {
            // called only once
            if (!csv_initialized) {
               // create columns
               for (uint64_t c_i = 0; c_i < BMCounters::COUNT; c_i++) {
                  csv_file << BMCounters::translation[c_i] << ", ";
               }

               for (uint64_t c_i = 0; c_i < WorkerCounters::COUNT; c_i++) {
                  csv_file << WorkerCounters::workerCounterTranslation[c_i] << " , ";
               }

               csv_file << "instructions/tx , ";
               csv_file << "L1-misses/tx , ";
               csv_file << "cycles/tx  , ";
               csv_file << "LLC-misses/tx , ";
               csv_file << "CPUs  , ";
               csv_file << "Workers ,";
               csv_file << "ProbSSD ,";
               csv_file << "Nodes ,";
               csv_file << "NodeId ,";
               csv_file << "PPThreads ,";
               csv_file << "Cooling ,";
               csv_file << "EvictCoolestEpoch ,";
               csv_file << "Free ,";
               csv_file << "Tag ,";
               wlInfo.csvHeader(csv_file);

               // -------------------------------------------------------------------------------------

               for (uint64_t c_i = 0; c_i < RDMACounters::COUNT - 1; c_i++) {
                  csv_file << RDMACounters::translation[c_i] << " ,  ";
               }
               csv_file << RDMACounters::translation[RDMACounters::COUNT - 1] << std::endl;
               csv_initialized = true;
            }

            // skip first second
            if(seconds >1){
               // print values
               for (uint64_t c_i = 0; c_i < BMCounters::COUNT; c_i++) {
                  csv_file << bmCounterAgg[c_i] << ", ";
               }

               for (uint64_t c_i = 0; c_i < WorkerCounters::COUNT; c_i++) {
                  if (c_i == WorkerCounters::latency && workerCounterAgg[WorkerCounters::tx_p] > 0) {
                     csv_file << workerCounterAgg[c_i] / (double)workerCounterAgg[WorkerCounters::tx_p] << " , ";

                  }else if (c_i == WorkerCounters::pp_latency_p1_incoming_requests &&
                            workerCounterAgg[WorkerCounters::pp_rounds] > 0)
                  {
                     csv_file << workerCounterAgg[c_i] 
                              << " , ";
                  }

                  else if (c_i == WorkerCounters::pp_latency_p2_cooling &&
                           workerCounterAgg[WorkerCounters::pp_rounds] > 0)
                  {
                     csv_file << workerCounterAgg[c_i] 
                              << " , ";
                  }
                  else if (c_i == WorkerCounters::pp_latency_p3_select &&
                           workerCounterAgg[WorkerCounters::pp_rounds] > 0)
                  {
                     csv_file << workerCounterAgg[c_i] 
                              << " , ";
                  }
                  else if (c_i == WorkerCounters::pp_latency_p4_send_requests &&
                           workerCounterAgg[WorkerCounters::pp_rounds] > 0)
                  {
                     csv_file << workerCounterAgg[c_i] 
                              << " , ";
                  }
                  else if (c_i == WorkerCounters::pp_latency_p5_incoming_responses &&
                           workerCounterAgg[WorkerCounters::pp_rounds] > 0)
                  {
                     csv_file << workerCounterAgg[c_i] 
                              << " , ";
                  }
                  else if (c_i == WorkerCounters::pp_latency_p6_send_responses &&
                           workerCounterAgg[WorkerCounters::pp_rounds] > 0)
                  {
                     csv_file << workerCounterAgg[c_i] 
                              << " , ";
                  

                  } else {
                     csv_file << workerCounterAgg[c_i] << " , ";
                  }
               }

               csv_file << cpuCountersAgg["instructions"] / tx_p << " , ";
               csv_file << cpuCountersAgg["L1-misses"] / tx_p << " , ";
               csv_file << cpuCountersAgg["cycles"] / tx_p << " , ";
               csv_file << cpuCountersAgg["LLC-misses"] / tx_p << " , ";
               csv_file << cpuCountersAgg["CPU"] << " , ";
               csv_file << FLAGS_worker << " , ";
               csv_file << FLAGS_prob_SSD << " , ";
               csv_file << FLAGS_nodes << " , ";
               csv_file << nodeId << " , ";
               csv_file << FLAGS_pageProviderThreads << " , ";
               csv_file << FLAGS_coolingPercentage << " , ";
               csv_file << FLAGS_evictCoolestEpochs << " , ";
               csv_file << FLAGS_freePercentage << " , ";
               csv_file << FLAGS_tag << " , ";
               // -------------------------------------------------------------------------------------
               // csv_file << wlInfo.experiment << " , ";
               // csv_file << wlInfo.elements << " , ";
               wlInfo.csv(csv_file);
               // -------------------------------------------------------------------------------------

               for (uint64_t c_i = 0; c_i < RDMACounters::COUNT - 1; c_i++) {
                  csv_file << rdmaCounterAgg[c_i] << " ,  ";
               }
               csv_file << rdmaCounterAgg[RDMACounters::COUNT - 1] << std::endl;
            }
         }
         // -------------------------------------------------------------------------------------
         // reset
         // -------------------------------------------------------------------------------------
         std::fill(workerCounterAgg.begin(), workerCounterAgg.end(), 0);
         std::fill(bmCounterAgg.begin(), bmCounterAgg.end(), 0);
         cpuCountersAgg.clear();
         std::this_thread::sleep_until(next);
         next += 1s;
      }
   }

   std::atomic<bool> running = true;
};

}  // namespace profiling
}  // namespace scalestore
