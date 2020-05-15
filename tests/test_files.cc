/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cmath>
#include <fstream>
#include <functional>
#include <future>

#include "common/api_data/gamma_engine_status.h"
#include "common/api_data/gamma_request.h"
#include "common/api_data/gamma_response.h"
#include "common/api_data/gamma_table.h"
#include "test.h"

/**
 * To run this demo, please download the ANN_SIFT10K dataset from
 *
 *   ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
 *
 * and unzip it.
 **/

namespace test {

static struct Options opt;

int AddDocToEngine(void *engine, int doc_num, int interval = 0) {
  for (int i = 0; i < doc_num; ++i) {
    tig_gamma::Doc doc;

    string url;
    for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
      tig_gamma::Field field;
      field.name = opt.fields_vec[j];
      field.datatype = opt.fields_type[j];
      char *value;
      int len = 0;

      string &data =
          opt.profiles[(uint64_t)opt.doc_id2 * opt.fields_vec.size() + j];
      if (opt.fields_type[j] == tig_gamma::DataType::INT) {
        value = static_cast<char *>(malloc(sizeof(int)));
        len = sizeof(int);
        int v = atoi(data.c_str());
        memcpy(value, &v, len);

        field.value = std::string(value, len);
        free(value);
      } else if (opt.fields_type[j] == tig_gamma::DataType::LONG) {
        value = static_cast<char *>(malloc(sizeof(long)));
        len = sizeof(long);
        long v = opt.doc_id;
        memcpy(value, &v, len);
        field.value = std::string(value, len);
        free(value);
      } else {
        field.value = data + "\001all";
        url = data;
      }

      field.source = url;

      doc.AddField(field);
    }

    tig_gamma::Field field;
    field.name = opt.vector_name;
    field.datatype = tig_gamma::DataType::VECTOR;
    field.source = url;
    int len = opt.d * sizeof(float);
    char *value = static_cast<char *>(malloc(len));
    memcpy((void *)value, (void *)(opt.feature + (uint64_t)opt.doc_id * opt.d),
           len);

    field.value = std::string(value, len);
    free(value);
    value = nullptr;

    doc.AddField(field);

    char *doc_str = nullptr;
    int doc_len = 0;
    doc.Serialize(&doc_str, &doc_len);
    AddOrUpdateDoc(engine, doc_str, doc_len);
    ++opt.doc_id;
    ++opt.doc_id2;
    if (interval > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(interval));
    }
  }
  return 0;
}

int SearchThread(void *engine, size_t num) {
  size_t idx = 0;
  double time = 0;
  int failed_count = 0;
  int req_num = 1;
  string error;
  while (idx < num) {
    double start = utils::getmillisecs();
    struct tig_gamma::VectorQuery vector_query;
    vector_query.name = opt.vector_name;

    int len = opt.d * sizeof(float) * req_num;
    char *value = reinterpret_cast<char *>(opt.feature + (uint64_t)idx * opt.d);

    vector_query.value = std::string(value, len);

    vector_query.min_score = 0;
    vector_query.max_score = 10000;
    vector_query.boost = 0.1;
    vector_query.has_boost = 0;

    tig_gamma::Request request;
    request.SetTopN(100);
    request.AddVectorQuery(vector_query);
    request.SetReqNum(req_num);
    request.SetDirectSearchType(0);
    // request.SetOnlineLogLevel("");
    request.SetHasRank(true);
    request.SetMultiVectorRank(0);
    request.SetL2Sqrt(false);

    if (opt.filter) {
      // string c1_lower = opt.profiles[idx * (opt.fields_vec.size()) + 4];
      // string c1_upper = opt.profiles[idx * (opt.fields_vec.size()) + 4];
      int low = 0;
      // long upper = 99999999999;
      int upper = 999999;
      string c1_lower = string((char *)&low, sizeof(low));
      string c1_upper = string((char *)&upper, sizeof(upper));

      {
        struct tig_gamma::RangeFilter range_filter;
        range_filter.field = "field2";
        range_filter.lower_value = string((char *)&low, sizeof(low));
        range_filter.upper_value = string((char *)&upper, sizeof(upper));
        range_filter.include_lower = false;
        range_filter.include_upper = true;
        request.AddRangeFilter(range_filter);
      }

      {
        struct tig_gamma::RangeFilter range_filter;
        range_filter.field = "field3";
        low = 0;
        upper = 999999;
        range_filter.lower_value = string((char *)&low, sizeof(low));
        range_filter.upper_value = string((char *)&upper, sizeof(upper));
        range_filter.include_lower = false;
        range_filter.include_upper = true;
        request.AddRangeFilter(range_filter);
      }

      tig_gamma::TermFilter term_filter;
      term_filter.field = "field3";
      term_filter.value = "1315\00115248";
      term_filter.is_union = true;

      request.AddTermFielter(term_filter);

      std::string field_name = "field1";
      request.AddField(field_name);

      std::string id = "_id";
      request.AddField(id);
    }

    char *request_str, *response_str;
    int request_len, response_len;

    request.Serialize(&request_str, &request_len);
    int ret =
        Search(engine, request_str, request_len, &response_str, &response_len);

    if (ret != 0) {
      LOG(ERROR) << "Search error [" << ret << "]";
    }
    free(request_str);

    tig_gamma::Response response;
    response.Deserialize(response_str, response_len);

    free(response_str);

    if (opt.print_doc) {
      std::vector<struct tig_gamma::SearchResult> &results = response.Results();
      for (size_t i = 0; i < results.size(); ++i) {
        int ii = idx + i;
        string msg = std::to_string(ii) + ", ";
        struct tig_gamma::SearchResult &result = results[i];

        std::vector<struct tig_gamma::ResultItem> &result_items =
            result.result_items;
        if (result_items.size() <= 0) {
          continue;
        }
        msg += string("total [") + std::to_string(result.total) + "], ";
        msg += string("result_num [") + std::to_string(result_items.size()) +
               "], ";
        for (size_t j = 0; j < result_items.size(); ++j) {
          struct tig_gamma::ResultItem &result_item = result_items[j];
          printDoc(result_item, msg, opt);
          msg += "\n";
        }
        if (abs(result_items[0].score - 1.0) < 0.001) {
          if (ii % 100 == 0) {
            LOG(INFO) << msg;
          }
        } else {
          if (!bitmap::test(opt.docids_bitmap_, ii)) {
            LOG(ERROR) << msg;
            error += std::to_string(ii) + ",";
            bitmap::set(opt.docids_bitmap_, ii);
            failed_count++;
          }
        }
      }
    }
    double elap = utils::getmillisecs() - start;
    time += elap;
    if (idx % 10000 == 0) {
      LOG(INFO) << "search time [" << time / 10000 << "]ms";
      time = 0;
    }
    idx += req_num;
    if (idx >= opt.doc_id) {
      idx = 0;
      break;
    }
  }
  LOG(ERROR) << error;
  return failed_count;
}

int GetVector(void *engine) {
  tig_gamma::Request request;
  request.SetTopN(10);
  request.SetReqNum(1);
  request.SetDirectSearchType(0);
  // request.SetOnlineLogLevel("");
  request.SetHasRank(true);
  request.SetMultiVectorRank(0);
  request.SetL2Sqrt(false);

  tig_gamma::TermFilter term_filter;
  term_filter.field = opt.vector_name;
  term_filter.value = "1.jpg";
  term_filter.is_union = false;

  request.AddTermFielter(term_filter);

  char *request_str, *response_str;
  int request_len, response_len;

  request.Serialize(&request_str, &request_len);
  int ret =
      Search(engine, request_str, request_len, &response_str, &response_len);

  free(request_str);

  tig_gamma::Response response;
  response.Deserialize(response_str, response_len);

  free(response_str);

  std::vector<struct tig_gamma::SearchResult> &results = response.Results();

  for (size_t i = 0; i < results.size(); ++i) {
    std::string msg = std::to_string(i) + ", ";
    struct tig_gamma::SearchResult &result = results[i];

    std::vector<struct tig_gamma::ResultItem> &result_items =
        result.result_items;
    if (result_items.size() <= 0) {
      continue;
    }
    msg += string("total [") + std::to_string(result.total) + "], ";
    msg += string("result_num [") + std::to_string(result_items.size()) + "], ";
    for (size_t j = 0; j < result_items.size(); ++j) {
      struct tig_gamma::ResultItem &result_item = result_items[j];
      printDoc(result_item, msg, opt);
      msg += "\n";
    }
    LOG(INFO) << msg;
  }
  return 0;
}

void UpdateThread(void *engine) {
  int doc_id = 0;
  tig_gamma::Doc doc;

  for (size_t j = 0; j < opt.fields_vec.size(); ++j) {
    tig_gamma::DataType data_type = opt.fields_type[j];
    std::string &name = opt.fields_vec[j];

    tig_gamma::Field field;
    field.name = name;
    field.source = "abc";
    field.datatype = data_type;

    std::string &data =
        opt.profiles[(uint64_t)doc_id * opt.fields_vec.size() + j];
    if (opt.fields_type[j] == tig_gamma::DataType::INT) {
      char *value_str = static_cast<char *>(malloc(sizeof(int)));
      int len = sizeof(int);
      int v = atoi("88");
      memcpy(value_str, &v, len);
      field.value = std::string(value_str, len);
      free(value_str);
    } else if (opt.fields_type[j] == tig_gamma::DataType::LONG) {
      char *value_str = static_cast<char *>(malloc(sizeof(long)));
      int len = sizeof(long);
      long v = atol(data.c_str());
      memcpy(value_str, &v, len);
      field.value = std::string(value_str, len);
      free(value_str);
    } else {
      field.value = data;
    }
    doc.AddField(field);
  }

  tig_gamma::Field field;
  field.name = opt.vector_name;
  field.source = "abc";
  field.datatype = tig_gamma::DataType::VECTOR;
  field.value = std::string(
      reinterpret_cast<char *>(opt.feature + (uint64_t)doc_id * opt.d),
      opt.d * sizeof(float));
  doc.AddField(field);

  char *doc_str = nullptr;
  int len = 0;
  doc.Serialize(&doc_str, &len);

  UpdateDoc(engine, doc_str, len);
  free(doc_str);
}

int InitEngine() {
#ifdef PERFORMANCE_TESTING
  int fd = open(opt.feature_file.c_str(), O_RDONLY, 0);
  size_t mmap_size = opt.add_doc_num * sizeof(float) * opt.d;
  opt.feature =
      static_cast<float *>(mmap(NULL, mmap_size, PROT_READ, MAP_SHARED, fd, 0));
  close(fd);
#else
  opt.feature = fvecs_read(opt.feature_file.c_str(), &opt.d, &opt.add_doc_num);
#endif

  std::cout << "n [" << opt.add_doc_num << "]" << std::endl;

  opt.add_doc_num =
      opt.add_doc_num > opt.max_doc_size ? opt.max_doc_size : opt.add_doc_num;

  int bitmap_bytes_size = 0;
  int ret =
      bitmap::create(opt.docids_bitmap_, bitmap_bytes_size, opt.max_doc_size);
  if (ret != 0) {
    LOG(ERROR) << "Create bitmap failed!";
  }

  assert(opt.docids_bitmap_ != nullptr);
  tig_gamma::Config config;
  config.SetPath(opt.path);
  config.SetLogDir(opt.log_dir);
  config.SetMaxDocSize(opt.max_doc_size);

  char *config_str = nullptr;
  int len = 0;
  config.Serialize(&config_str, &len);
  opt.engine = Init(config_str, len);
  free(config_str);
  config_str = nullptr;

  assert(opt.engine != nullptr);
  return 0;
}

int Create() {
  tig_gamma::Table table;
  table.SetName(opt.vector_name);
  table.SetVectorsNum(1);
  table.SetRetrievalType(opt.retrieval_type);
  table.SetRetrievalParam(kIVFPQParam);
  table.SetIndexingSize(opt.indexing_size);

  for (size_t i = 0; i < opt.fields_vec.size(); ++i) {
    struct tig_gamma::FieldInfo field_info;
    field_info.name = opt.fields_vec[i];

    char is_index = 0;
    if (opt.filter && (i == 0 || i == 2 || i == 3 || i == 4)) {
      is_index = 1;
    }
    field_info.is_index = is_index;
    field_info.data_type = opt.fields_type[i];
    table.AddField(field_info);
  }

  struct tig_gamma::VectorInfo vector_info;
  vector_info.name = opt.vector_name;
  vector_info.data_type = tig_gamma::DataType::FLOAT;
  vector_info.is_index = true;
  vector_info.dimension = opt.d;
  vector_info.model_id = opt.model_id;
  vector_info.store_type = opt.store_type;
  vector_info.store_param = "{\"cache_size\": 2048}";
  vector_info.has_source = false;

  table.AddVectorInfo(vector_info);

  char *table_str = nullptr;
  int len = 0;
  table.Serialize(&table_str, &len);

  int ret = CreateTable(opt.engine, table_str, len);

  free(table_str);

  return ret;
}

int Add() {
  size_t idx = 0;

  std::ifstream fin;
  fin.open(opt.profile_file.c_str());
  std::string str;
  while (idx < opt.add_doc_num) {
    std::getline(fin, str);
    if (str == "") break;
    auto profile = std::move(utils::split(str, "\t"));
    size_t i = 0;
    for (const auto &p : profile) {
      opt.profiles[idx * opt.fields_vec.size() + i] = p;
      ++i;
      if (i > opt.fields_vec.size() - 1) {
        break;
      }
    }

    ++idx;
  }
  fin.close();

  int ret = AddDocToEngine(opt.engine, opt.add_doc_num);
  return ret;
}

int BuildEngineIndex() {
  // BuildIndex(opt.engine);
  int n_index_status = -1;
  do {
    char *status = nullptr;
    int len = 0;
    GetEngineStatus(opt.engine, &status, &len);
    tig_gamma::EngineStatus engine_status;
    engine_status.Deserialize(status, len);
    free(status);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    n_index_status = engine_status.IndexStatus();
  } while (n_index_status != 2);

  // string docid = "1.jpg";
  // ByteArray *value = StringToByteArray(docid);
  // Doc *doc = GetDocByID(opt.engine, value);
  // DelDoc(opt.engine, value);
  // doc = GetDocByID(opt.engine, value);

  LOG(INFO) << "Indexed!";
  // UpdateThread(opt.engine);
  GetVector(opt.engine);
  // opt.doc_id = 0;
  opt.doc_id2 = 1;
  return 0;
}

int Search() {
  std::thread t_searchs[opt.search_thread_num];

  std::function<int()> func_search =
      std::bind(SearchThread, opt.engine, opt.search_num);
  std::future<int> search_futures[opt.search_thread_num];
  std::packaged_task<int()> tasks[opt.search_thread_num];

  double start = utils::getmillisecs();
  for (int i = 0; i < opt.search_thread_num; ++i) {
    tasks[i] = std::packaged_task<int()>(func_search);
    search_futures[i] = tasks[i].get_future();
    t_searchs[i] = std::thread(std::move(tasks[i]));
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));

  // std::function<int(void *)> add_func =
  //     std::bind(AddDocToEngine, std::placeholders::_1, 1 * 1, 1);
  // std::thread add_thread(add_func, opt.engine);

  // get search results
  for (int i = 0; i < opt.search_thread_num; ++i) {
    search_futures[i].wait();
    int error_num = search_futures[i].get();
    if (error_num != 0) {
      LOG(ERROR) << "error_num [" << error_num << "]";
    }
    t_searchs[i].join();
  }

  double end = utils::getmillisecs();
  LOG(INFO) << "Search cost [" << end - start << "] ms";
  // add_thread.join();
  return 0;
}

int DumpEngine() {
  int ret = Dump(opt.engine);

  // ret = AddDocToEngine(opt.engine, opt.add_doc_num);

  // std::this_thread::sleep_for(std::chrono::seconds(10));

  // ret = Dump(opt.engine);

  Close(opt.engine);
  opt.engine = nullptr;
  delete opt.docids_bitmap_;
  return ret;
}

int LoadEngine() {
  int bitmap_bytes_size = 0;
  int ret =
      bitmap::create(opt.docids_bitmap_, bitmap_bytes_size, opt.max_doc_size);
  if (ret != 0) {
    LOG(ERROR) << "Create bitmap failed!";
  }
  tig_gamma::Config config;
  config.SetMaxDocSize(opt.max_doc_size);
  config.SetPath(opt.path);

  char *config_str;
  int config_len;
  config.Serialize(&config_str, &config_len);

  opt.engine = Init(config_str, config_len);

  ret = Load(opt.engine);
  return ret;
}

int DumpAfterLoad() {
  int ret = Dump(opt.engine);
  return ret;
}

int CloseEngine() {
  Close(opt.engine);
  opt.engine = nullptr;
  delete opt.docids_bitmap_;
#ifdef PERFORMANCE_TESTING
  munmap(opt.feature, opt.add_doc_num * sizeof(float) * opt.d);
#else
  delete opt.feature;
#endif
  return 0;
}

}  // namespace test

int main(int argc, char **argv) {
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  if (argc != 3) {
    std::cout << "Usage: [Program] [profile_file] [vectors_file]\n";
    return 1;
  }
  test::opt.profile_file = argv[1];
  test::opt.feature_file = argv[2];
  std::cout << test::opt.profile_file.c_str() << " "
            << test::opt.feature_file.c_str() << std::endl;
  test::InitEngine();
  test::Create();
  test::Add();
  test::BuildEngineIndex();
  // test::Add();
  test::Search();
  // test::DumpEngine();
  // test::LoadEngine();
  // test::BuildEngineIndex();
  // test::Search();
  // test::DumpAfterLoad();
  test::CloseEngine();

  return 0;
}
