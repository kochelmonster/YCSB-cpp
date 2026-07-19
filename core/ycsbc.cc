//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <chrono>
#include <cstring>
#include <ctime>
#include <future>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "client.h"
#include "core/dataset.h"
#include "core_workload.h"
#include "db_factory.h"
#include "measurements.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/timer.h"
#include "utils/utils.h"

void UsageMessage(const char* command);
bool StrStartWith(const char* str, const char* pre);
void ParseCommandLine(int argc, const char* argv[],
                      ycsbc::utils::Properties& props);

void StatusThread(ycsbc::Measurements* measurements,
                  ycsbc::utils::CountDownLatch* latch, int interval) {
  using namespace std::chrono;
  time_point<system_clock> start = system_clock::now();
  bool done = false;
  while (1) {
    time_point<system_clock> now = system_clock::now();
    std::time_t now_c = system_clock::to_time_t(now);
    duration<double> elapsed_time = now - start;

    std::cout << std::put_time(std::localtime(&now_c), "%F %T") << ' '
              << static_cast<long long>(elapsed_time.count()) << " sec: ";

    std::cout << measurements->GetStatusMsg() << std::endl;

    if (done) {
      break;
    }
    done = latch->AwaitFor(interval);
  };
}

void RateLimitThread(std::string rate_file,
                     std::vector<ycsbc::utils::RateLimiter*> rate_limiters,
                     ycsbc::utils::CountDownLatch* latch) {
  std::ifstream ifs;
  ifs.open(rate_file);

  if (!ifs.is_open()) {
    ycsbc::utils::Exception("failed to open: " + rate_file);
  }

  int64_t num_threads = rate_limiters.size();

  int64_t last_time = 0;
  while (!ifs.eof()) {
    int64_t next_time;
    int64_t next_rate;
    ifs >> next_time >> next_rate;

    if (next_time <= last_time) {
      ycsbc::utils::Exception("invalid rate file");
    }

    bool done = latch->AwaitFor(next_time - last_time);
    if (done) {
      break;
    }
    last_time = next_time;

    for (auto x : rate_limiters) {
      x->SetRate(next_rate / num_threads);
    }
  }
}

int main(const int argc, const char* argv[]) {
  ycsbc::utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction =
      (props.GetProperty("dotransaction", "false") == "true");
  if (!do_load && !do_transaction) {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));

  ycsbc::Measurements* measurements = ycsbc::CreateMeasurements(&props);
  if (measurements == nullptr) {
    std::cerr << "Unknown measurements name" << std::endl;
    exit(1);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  // print status periodically
  const bool show_status = (props.GetProperty("status", "false") == "true");
  const int status_interval =
      std::stoi(props.GetProperty("status.interval", "10"));

  // load phase
  if (do_load) {
    const int total_ops =
        stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    // Pre-generate all keys and values outside the timed window so the
    // measurement reflects DB cost, not YCSB framework cost (BuildKeyName,
    // BuildValues, RNG, ...).
    ycsbc::Dataset load_dataset(props, wl, 0, true);
    const bool force_generate =
        ycsbc::utils::StrToBool(props.GetProperty("force_generate", "false"));
    const std::string load_dataset_path = load_dataset.GetFullPath(total_ops);
    bool should_generate = false;
    std::string reason;
    if (force_generate) {
      should_generate = true;
      reason = "force_generate=true";
    } else {
      struct stat buffer;
      if (stat(load_dataset_path.c_str(), &buffer) != 0) {
        should_generate = true;
        reason = "dataset file missing";
      } else if (!load_dataset.IsValidForOpCount(total_ops)) {
        should_generate = true;
        reason = "dataset header count mismatch";
      }
    }

    if (should_generate) {
      std::cout << "Generating load dataset: " << reason << " ("
                << load_dataset_path << ")" << std::endl;
      load_dataset.Generate(total_ops);
    } else {
      std::cout << "Reusing existing load dataset: " << load_dataset_path
                << std::endl;
    }
    load_dataset.Open(total_ops);

    ycsbc::utils::Timer<double> timer;

    // Use a dedicated DB instance for the load phase
    ycsbc::DB* load_db = ycsbc::DBFactory::CreateDB(&props, measurements);
    if (load_db == nullptr) {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    load_db->Init();

    const std::string& table = wl.table_name();

    timer.Start();
    load_db->Load(table, load_dataset);
    double runtime = timer.End();

    std::cout << "Load runtime(sec): " << runtime << std::endl;
    std::cout << "Load operations(ops): " << total_ops << std::endl;
    std::cout << "Load throughput(ops/sec): " << total_ops / runtime << std::endl;

    load_db->Cleanup();
    delete load_db;
  }

  measurements->Reset();
  std::this_thread::sleep_for(
      std::chrono::seconds(stoi(props.GetProperty("sleepafterload", "0"))));

  // transaction phase
  if (do_transaction) {
    // initial ops per second, unlimited if <= 0
    const int64_t ops_limit = std::stoi(props.GetProperty("limit.ops", "0"));
    // rate file path for dynamic rate limiting, format "time_stamp_sec
    // new_ops_per_second" per line
    std::string rate_file = props.GetProperty("limit.file", "");

    const int total_ops =
        stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    // Compute per-thread op counts up front (needed for pre-generation)
    std::vector<int> txn_thread_ops(num_threads);
    for (int i = 0; i < num_threads; ++i) {
      txn_thread_ops[i] = total_ops / num_threads;
      if (i < total_ops % num_threads) txn_thread_ops[i]++;
    }

    // Pre-generate all keys and values outside the timed window so the
    // measurement reflects DB cost, not YCSB framework cost.
    std::vector<std::unique_ptr<ycsbc::Dataset>> txn_datasets;
    const bool force_generate =
        ycsbc::utils::StrToBool(props.GetProperty("force_generate", "false"));
    for (int i = 0; i < num_threads; ++i) {
      txn_datasets.emplace_back(new ycsbc::Dataset(props, wl, i, false));
      ycsbc::Dataset& dataset = *txn_datasets.back();
      const int thread_ops = txn_thread_ops[i];
      const std::string dataset_path = dataset.GetFullPath(thread_ops);

      bool should_generate = false;
      std::string reason;
      if (force_generate) {
        should_generate = true;
        reason = "force_generate=true";
      } else {
        struct stat buffer;
        if (stat(dataset_path.c_str(), &buffer) != 0) {
          should_generate = true;
          reason = "dataset file missing";
        } else if (!dataset.IsValidForOpCount(thread_ops)) {
          should_generate = true;
          reason = "dataset header count mismatch";
        }
      }

      if (should_generate) {
        std::cout << "Generating transaction dataset for thread " << i << ": "
                  << reason << " (" << dataset_path << ")" << std::endl;
        txn_datasets.back()->Generate(txn_thread_ops[i]);
      } else {
        std::cout << "Reusing existing transaction dataset for thread " << i
                  << ": " << dataset_path << std::endl;
      }
      //txn_datasets.back()->DEBUG(txn_thread_ops[i]);
      txn_datasets.back()->Open(txn_thread_ops[i]);
    }

    // Create fresh DB instances for the transaction phase
    std::vector<ycsbc::DB*> dbs;
    for (int i = 0; i < num_threads; i++) {
      ycsbc::DB* db = ycsbc::DBFactory::CreateDB(&props, measurements);
      if (db == nullptr) {
        std::cerr << "Unknown database name " << props["dbname"] << std::endl;
        exit(1);
      }
      dbs.push_back(db);
    }

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    // Initialize all DB instances before the timed window
    for (int i = 0; i < num_threads; ++i) {
      dbs[i]->Init();
    }

    std::future<void> status_future;
    if (show_status) {
      status_future = std::async(std::launch::async, StatusThread, measurements,
                                 &latch, status_interval);
    }
    std::vector<std::future<int>> client_threads;
    std::vector<ycsbc::utils::RateLimiter*> rate_limiters;
    
    timer.Start();
    for (int i = 0; i < num_threads; ++i) {
      ycsbc::utils::RateLimiter* rlim = nullptr;
      if (ops_limit > 0 || rate_file != "") {
        int64_t per_thread_ops = ops_limit / num_threads;
        rlim = new ycsbc::utils::RateLimiter(per_thread_ops, per_thread_ops);
      }
      rate_limiters.push_back(rlim);
      client_threads.emplace_back(
          std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                     txn_thread_ops[i], &latch, rlim,
                     txn_datasets[i].get()));
    }

    std::future<void> rlim_future;
    if (rate_file != "") {
      rlim_future = std::async(std::launch::async, RateLimitThread, rate_file,
                               rate_limiters, &latch);
    }

    assert((int)client_threads.size() == num_threads);

    int sum = 0;
    for (auto& n : client_threads) {
      assert(n.valid());
      sum += n.get();
    }
    double runtime = timer.End();

    if (show_status) {
      status_future.wait();
    }

    std::cout << measurements->GetStatusMsg() << std::endl;
    std::cout << "Run runtime(sec): " << runtime << std::endl;
    std::cout << "Run operations(ops): " << sum << std::endl;
    std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;

    // Cleanup and delete all DB instances after the timed window
    for (int i = 0; i < num_threads; ++i) {
      dbs[i]->Cleanup();
      delete dbs[i];
    }
  }
}

void ParseCommandLine(int argc, const char* argv[],
                      ycsbc::utils::Properties& props) {
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-load") == 0) {
      props.SetProperty("doload", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-run") == 0 ||
               strcmp(argv[argindex], "-t") == 0) {
      props.SetProperty("dotransaction", "true");
      argindex++;
    } else if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -P" << std::endl;
        exit(0);
      }
      std::string filename(argv[argindex]);
      std::ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const std::string& message) {
        std::cerr << message << std::endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else if (strcmp(argv[argindex], "-p") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos) {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)"
                  << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)),
                        ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    } else if (strcmp(argv[argindex], "-s") == 0) {
      props.SetProperty("status", "true");
      argindex++;
    } else {
      UsageMessage(argv[0]);
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char* command) {
  std::cout
      << "Usage: " << command
      << " [options]\n"
         "Options:\n"
         "  -load: run the loading phase of the workload\n"
         "  -t: run the transactions phase of the workload\n"
         "  -run: same as -t\n"
         "  -threads n: execute using n threads (default: 1)\n"
         "  -db dbname: specify the name of the DB to use (default: basic)\n"
         "  -P propertyfile: load properties from the given file. Multiple "
         "files can\n"
         "                   be specified, and will be processed in the order "
         "specified\n"
         "  -p name=value: specify a property to be passed to the DB and "
         "workloads\n"
         "                 multiple properties can be specified, and override "
         "any\n"
         "                 values in the propertyfile\n"
         "  -s: print status every 10 seconds (use status.interval prop to "
         "override)"
      << std::endl;
}

inline bool StrStartWith(const char* str, const char* pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}
