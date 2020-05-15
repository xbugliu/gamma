/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_engine.h"

#include <fcntl.h>
#include <locale.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <thread>
#include <vector>

#include "bitmap.h"
#include "cJSON.h"
#include "gamma_common_data.h"
#include "gamma_table_io.h"
#include "log.h"
#include "utils.h"

using std::string;

namespace tig_gamma {

#ifdef DEBUG
static string float_array_to_string(float *data, int len) {
  if (data == nullptr) return "";
  std::stringstream ss;
  ss << "[";
  for (int i = 0; i < len; ++i) {
    ss << data[i];
    if (i != len - 1) {
      ss << ",";
    }
  }
  ss << "]";
  return ss.str();
}

static string VectorQueryToString(VectorQuery *vector_query) {
  std::stringstream ss;
  ss << "name:"
     << std::string(vector_query->name->value, vector_query->name->len)
     << " min score:" << vector_query->min_score
     << " max score:" << vector_query->max_score
     << " boost:" << vector_query->boost
     << " has boost:" << vector_query->has_boost << " value:"
     << float_array_to_string((float *)vector_query->value->value,
                              vector_query->value->len / sizeof(float));
  return ss.str();
}

// static string RequestToString(const Request *request) {
//   std::stringstream ss;
//   ss << "{req_num:" << request->req_num << " topn:" << request->topn
//      << " has_rank:" << request->has_rank
//      << " vec_num:" << request->vec_fields_num;
//   for (int i = 0; i < request->vec_fields_num; ++i) {
//     ss << " vec_id:" << i << " [" <<
//     VectorQueryToString(request->vec_fields[i])
//        << "]";
//   }
//   ss << "}";
//   return ss.str();
// }
#endif  // DEBUG

GammaEngine::GammaEngine(const string &index_root_path)
    : index_root_path_(index_root_path),
      date_time_format_("%Y-%m-%d-%H:%M:%S") {
  docids_bitmap_ = nullptr;
  profile_ = nullptr;
  vec_manager_ = nullptr;
  index_status_ = IndexStatus::UNINDEXED;
  delete_num_ = 0;
  b_running_ = false;
  b_field_running_ = false;
  dump_docid_ = 0;
  bitmap_bytes_size_ = 0;
  field_range_index_ = nullptr;
  created_table_ = false;
  indexed_field_num_ = 0;
  b_loading_ = false;
#ifdef PERFORMANCE_TESTING
  search_num_ = 0;
#endif
  counters_ = nullptr;
}

GammaEngine::~GammaEngine() {
  if (b_running_) {
    b_running_ = false;
    std::mutex running_mutex;
    std::unique_lock<std::mutex> lk(running_mutex);
    running_cv_.wait(lk);
  }

  if (b_field_running_) {
    b_field_running_ = false;
    std::mutex running_mutex;
    std::unique_lock<std::mutex> lk(running_mutex);
    running_field_cv_.wait(lk);
  }

  if (vec_manager_) {
    delete vec_manager_;
    vec_manager_ = nullptr;
  }

  if (profile_) {
    delete profile_;
    profile_ = nullptr;
  }

  if (docids_bitmap_) {
    delete docids_bitmap_;
    docids_bitmap_ = nullptr;
  }

  if (field_range_index_) {
    delete field_range_index_;
    field_range_index_ = nullptr;
  }
  if (counters_) delete counters_;
}

GammaEngine *GammaEngine::GetInstance(const string &index_root_path,
                                      int max_doc_size) {
  GammaEngine *engine = new GammaEngine(index_root_path);
  int ret = engine->Setup(max_doc_size);
  if (ret < 0) {
    LOG(ERROR) << "BuildSearchEngine [" << index_root_path << "] error!";
    return nullptr;
  }
  return engine;
}

int GammaEngine::Setup(int max_doc_size) {
  if (max_doc_size < 1) {
    return -1;
  }
  max_doc_size_ = max_doc_size;

  if (!utils::isFolderExist(index_root_path_.c_str())) {
    mkdir(index_root_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  dump_path_ = index_root_path_ + "/dump";

  std::string::size_type pos = index_root_path_.rfind('/');
  pos = pos == std::string::npos ? 0 : pos + 1;
  string dir_name = index_root_path_.substr(pos);
  string vearch_backup_path = "/tmp/vearch";
  string engine_backup_path = vearch_backup_path + "/" + dir_name;
  dump_backup_path_ = engine_backup_path + "/dump";
  utils::make_dir(vearch_backup_path.c_str());
  utils::make_dir(engine_backup_path.c_str());
  utils::make_dir(dump_backup_path_.c_str());

  if (!docids_bitmap_) {
    if (bitmap::create(docids_bitmap_, bitmap_bytes_size_, max_doc_size) != 0) {
      LOG(ERROR) << "Cannot create bitmap!";
      return -1;
    }
  }

  if (!profile_) {
    profile_ = new Profile(max_doc_size, index_root_path_);
    if (!profile_) {
      LOG(ERROR) << "Cannot create profile!";
      return -2;
    }
  }

  counters_ = new GammaCounters(&max_docid_, &delete_num_);
  if (!vec_manager_) {
    vec_manager_ = new VectorManager(IVFPQ, Mmap, docids_bitmap_, max_doc_size,
                                     index_root_path_, counters_);
    if (!vec_manager_) {
      LOG(ERROR) << "Cannot create vec_manager!";
      return -3;
    }
  }

  max_docid_ = 0;
  LOG(INFO) << "GammaEngine setup successed!";
  return 0;
}

int GammaEngine::Search(Request &request, Response &response_results) {
#ifdef DEBUG
  // LOG(INFO) << "search request:" << RequestToString(request);
#endif

  int ret = 0;
  int req_num = request.ReqNum();

  if (req_num <= 0) {
    string msg = "req_num should not less than 0";
    LOG(ERROR) << msg;
    return -1;
  }

  int topn = request.TopN();
  
  std::string online_log_level = request.OnlineLogLevel();

  utils::OnlineLogger logger;
  if (0 != logger.Init(online_log_level)) {
    LOG(WARNING) << "init online logger error!";
  }

  OLOG(&logger, INFO, "online log level: " << online_log_level);

  bool use_direct_search = ((request.DirectSearchType() == 1) ||
                            ((request.DirectSearchType() == 0) &&
                             (index_status_ != IndexStatus::INDEXED)));

  if ((not use_direct_search) && (index_status_ != IndexStatus::INDEXED)) {
    string msg = "index not trained!";
    LOG(ERROR) << msg;
    for (int i = 0; i < req_num; ++i) {
      SearchResult result;
      result.msg = msg;
      result.result_code = SearchResultCode::INDEX_NOT_TRAINED;
      response_results.AddResults(result);
    }
    return -2;
  }

  std::vector<struct VectorQuery> &vec_fields = request.VecFields();
  GammaQuery gamma_query;
  gamma_query.logger = &logger;
  gamma_query.vec_query = vec_fields;

  GammaSearchCondition condition;
  condition.topn = topn;
  condition.parallel_mode = 1;  // default to parallelize over inverted list
  condition.recall_num = topn;  // TODO: recall number should be
                                // transmitted from search request
  condition.multi_vector_rank = request.MultiVectorRank() == 1 ? true : false;
  condition.has_rank = request.HasRank() == 1 ? true : false;
  condition.parallel_based_on_query = request.ParallelBasedOnQuery();
  condition.use_direct_search = use_direct_search;
  condition.l2_sqrt = request.L2Sqrt();

#ifdef BUILD_GPU
  condition.range_filters_num = request->range_filters_num;
  condition.range_filters = request->range_filters;
  condition.term_filters_num = request->term_filters_num;
  condition.term_filters = request->term_filters;
  condition.profile = profile_;
#endif  // BUILD_GPU

#ifndef BUILD_GPU
  MultiRangeQueryResults range_query_result;
  std::vector<struct RangeFilter> &range_filters = request.RangeFilters();
  size_t range_filters_num = range_filters.size();

  std::vector<struct TermFilter> &term_filters = request.TermFilters();
  size_t term_filters_num = term_filters.size();
  if (range_filters_num > 0 || term_filters_num > 0) {
    int num = MultiRangeQuery(request, condition, response_results,
                              &range_query_result, logger);
    if (num == 0) {
      return 0;
    }
  }
#ifdef PERFORMANCE_TESTING
  condition.Perf("filter");
#endif
#endif

  gamma_query.condition = &condition;

  size_t vec_fields_num = vec_fields.size();
  if (vec_fields_num > 0) {
    GammaResult gamma_results[req_num];
    int doc_num = GetDocsNum();

    for (int i = 0; i < req_num; ++i) {
      gamma_results[i].total = doc_num;
    }

    ret = vec_manager_->Search(gamma_query, gamma_results);
    if (ret != 0) {
      string msg = "search error [" + std::to_string(ret) + "]";
      for (int i = 0; i < req_num; ++i) {
        SearchResult result;
        result.msg = msg;
        result.result_code = SearchResultCode::SEARCH_ERROR;
        response_results.AddResults(result);
      }

      const char *log_message = logger.Data();
      if (log_message) {
        response_results.SetOnlineLogMessage(
            std::string(log_message, logger.Length()));
      }

      return -3;
    }

#ifdef PERFORMANCE_TESTING
    condition.Perf("search total");
#endif
    PackResults(gamma_results, response_results, request);
#ifdef PERFORMANCE_TESTING
    condition.Perf("pack results");
#endif

#ifdef BUILD_GPU
  }
#else
  } else {
    GammaResult gamma_result;
    gamma_result.topn = topn;

    std::vector<std::pair<string, int>> fields_ids;
    std::vector<string> vec_names;

    const auto range_result = range_query_result.GetAllResult();
    if (range_result == nullptr && term_filters_num > 0) {
      for (size_t i = 0; i < term_filters_num; ++i) {
        struct TermFilter &term_filter = term_filters[i];

        string value = term_filter.field;
        long key = -1;
        memcpy(&key, term_filter.value.c_str(), sizeof(key));

        int doc_id = -1;
        int ret = profile_->GetDocIDByKey(key, doc_id);
        if (ret != 0) {
          continue;
        }

        fields_ids.emplace_back(std::make_pair(value, doc_id));
        vec_names.emplace_back(std::move(value));
      }
      if (fields_ids.size() > 0) {
        gamma_result.init(topn, vec_names.data(), fields_ids.size());
        std::vector<string> vec;
        int ret = vec_manager_->GetVector(fields_ids, vec);
        if (ret == 0) {
          int idx = 0;
          VectorDoc *doc = gamma_result.docs[gamma_result.results_count];
          for (const auto &field_id : fields_ids) {
            int id = field_id.second;
            doc->docid = id;
            doc->fields[idx].name = vec[idx];
            doc->fields[idx].source = nullptr;
            doc->fields[idx].source_len = 0;
            ++idx;
          }
          ++gamma_result.results_count;
          gamma_result.total = 1;
        }
      }
    } else {
      gamma_result.init(topn, nullptr, 0);
      for (int docid = 0; docid < max_docid_; ++docid) {
        if (range_query_result.Has(docid) &&
            !bitmap::test(docids_bitmap_, docid)) {
          ++gamma_result.total;
          if (gamma_result.results_count < topn) {
            gamma_result.docs[gamma_result.results_count++]->docid = docid;
          }
        }
      }
    }
    // response_results.req_num = 1;  // only one result
    PackResults(&gamma_result, response_results, request);
  }
#endif

#ifdef PERFORMANCE_TESTING
  LOG(INFO) << condition.OutputPerf().str();
#endif

  const char *log_message = logger.Data();
  if (log_message) {
    response_results.SetOnlineLogMessage(
        std::string(log_message, logger.Length()));
  }

  return ret;
}

int GammaEngine::MultiRangeQuery(Request &request,
                                 GammaSearchCondition &condition,
                                 Response &response_results,
                                 MultiRangeQueryResults *range_query_result,
                                 utils::OnlineLogger &logger) {
  std::vector<FilterInfo> filters;
  std::vector<struct RangeFilter> &range_filters = request.RangeFilters();
  std::vector<struct TermFilter> &term_filters = request.TermFilters();

  int range_filters_size = range_filters.size();
  int term_filters_size = term_filters.size();

  filters.resize(range_filters_size + term_filters_size);
  int idx = 0;

  for (int i = 0; i < range_filters_size; ++i) {
    struct RangeFilter &filter = range_filters[i];

    filters[idx].field = profile_->GetAttrIdx(filter.field);
    filters[idx].lower_value = filter.lower_value;
    filters[idx].upper_value = filter.upper_value;

    ++idx;
  }

  for (int i = 0; i < term_filters_size; ++i) {
    struct TermFilter &filter = term_filters[i];

    filters[idx].field = profile_->GetAttrIdx(filter.field);
    filters[idx].lower_value = filter.value;
    filters[idx].is_union = filter.is_union;

    ++idx;
  }

  int retval = field_range_index_->Search(filters, range_query_result);

  OLOG(&logger, DEBUG, "search numeric index, ret: " << retval);

  if (retval == 0) {
    string msg = "No result: numeric filter return 0 result";
    LOG(INFO) << msg;
    for (int i = 0; i < request.ReqNum(); ++i) {
      SearchResult result;
      result.msg = msg;
      result.result_code = SearchResultCode::SUCCESS;
      response_results.AddResults(result);
    }

    const char *log_message = logger.Data();
    if (log_message) {
      response_results.SetOnlineLogMessage(
          std::string(log_message, logger.Length()));
    }
  } else if (retval < 0) {
    condition.range_query_result = nullptr;
  } else {
    condition.range_query_result = range_query_result;
  }
  return retval;
}

int GammaEngine::CreateTable(Table &table) {
  if (!vec_manager_ || !profile_) {
    LOG(ERROR) << "vector and profile should not be null!";
    return -1;
  }
  int ret_vec = vec_manager_->CreateVectorTable(table);
  int ret_profile = profile_->CreateTable(table);

  indexing_size_ = table.IndexingSize();

  if (ret_vec != 0 || ret_profile != 0) {
    LOG(ERROR) << "Cannot create table!";
    return -2;
  }

#ifndef BUILD_GPU
  field_range_index_ = new MultiFieldsRangeIndex(index_root_path_, profile_);
  if ((nullptr == field_range_index_) || (AddNumIndexFields() < 0)) {
    LOG(ERROR) << "add numeric index fields error!";
    return -3;
  }

  auto func_build_field_index = std::bind(&GammaEngine::BuildFieldIndex, this);
  std::thread t(func_build_field_index);
  t.detach();
#endif
  std::string table_name = table.Name();
  std::string path = index_root_path_ + "/" + table_name + ".schema";
  TableIO tio(path);  // rewrite it if the path is already existed
  if (tio.Write(table)) {
    LOG(ERROR) << "write table schema error, path=" << path;
  }
  LOG(INFO) << "create table [" << table_name << "] success!";
  created_table_ = true;
  return 0;
}

int GammaEngine::Add(Doc *doc) {
  if (max_docid_ >= max_doc_size_) {
    LOG(ERROR) << "Doc size reached upper size [" << max_docid_ << "]";
    return -1;
  }
  std::vector<struct Field> &fields = doc->Fields();
  std::vector<struct Field> fields_profile, fields_vec;

  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].datatype != DataType::VECTOR) {
      fields_profile.emplace_back(fields[i]);
    } else {
      fields_vec.emplace_back(fields[i]);
    }
  }
  // add fields into profile
  if (profile_->Add(fields_profile, max_docid_, false) != 0) {
    return -1;
  }

  // for (int i = 0; i < doc->fields_num; ++i) {
  //   auto *f = doc->fields[i];
  //   if (f->data_type == VECTOR) {
  //     continue;
  //   }
  //   int idx = profile_->GetAttrIdx(string(f->name->value, f->name->len));
  //   field_range_index_->Add(max_docid_, idx);
  // }

  // add vectors by VectorManager
  if (vec_manager_->AddToStore(max_docid_, fields_vec) != 0) {
    return -2;
  }
  ++max_docid_;

  return 0;
}

int GammaEngine::AddOrUpdate(Doc &doc) {
  if (max_docid_ >= max_doc_size_) {
    LOG(ERROR) << "Doc size reached upper size [" << max_docid_ << "]";
    return -1;
  }
#ifdef PERFORMANCE_TESTING
  double start = utils::getmillisecs();
#endif
  std::vector<struct Field> fields_profile, fields_vec;
  long key = -1;

  std::vector<struct Field> &fields = doc.Fields();
  size_t fields_num = fields.size();
  for (size_t i = 0; i < fields_num; ++i) {
    struct Field &field = fields[i];
    if (field.datatype != DataType::VECTOR) {
      fields_profile.emplace_back(field);
      const string &name = field.name;
      if (name == "_id") {
        memcpy(&key, field.value.c_str(), sizeof(key));
      }
    } else {
      fields_vec.emplace_back(field);
    }
  }
  // add fields into profile
  int docid = -1;
  profile_->GetDocIDByKey(key, docid);
  if (docid == -1) {
    int ret = profile_->Add(fields_profile, max_docid_, false);
    if (ret != 0) return -2;
  } else {
    if (Update(docid, fields_profile, fields_vec)) {
      LOG(ERROR) << "update error, key=" << key << ", docid=" << docid;
      return -3;
    }
    return 0;
  }
#ifdef PERFORMANCE_TESTING
  double end_profile = utils::getmillisecs();
#endif
  // for (int i = 0; i < doc->fields_num; ++i) {
  //   auto *f = doc->fields[i];
  //   if (f->data_type == VECTOR) {
  //     continue;
  //   }
  //   int idx = profile_->GetAttrIdx(string(f->name->value, f->name->len));
  //   field_range_index_->Add(max_docid_, idx);
  // }

  // add vectors by VectorManager
  if (vec_manager_->AddToStore(max_docid_, fields_vec) != 0) {
    return -4;
  }
  if (not b_running_ and index_status_ == UNINDEXED) {
    if (max_docid_ >= indexing_size_) {
      LOG(INFO) << "Begin indexing.";
      this->BuildIndex();
    }
  }
  ++max_docid_;
#ifdef PERFORMANCE_TESTING
  double end = utils::getmillisecs();
  if (max_docid_ % 10000 == 0) {
    LOG(INFO) << "profile cost [" << end_profile - start
              << "]ms, vec store cost [" << end - end_profile << "]ms";
  }
#endif
  return 0;
}

int GammaEngine::Update(Doc *doc) { return -1; }

int GammaEngine::Update(int doc_id, std::vector<struct Field> &fields_profile,
                        std::vector<struct Field> &fields_vec) {
  int ret = vec_manager_->Update(doc_id, fields_vec);
  if (ret != 0) {
    return ret;
  }

#ifndef BUILD_GPU
  for (size_t i = 0; i < fields_profile.size(); ++i) {
    struct Field &field = fields_profile[i];
    int idx = profile_->GetAttrIdx(field.name);
    field_range_index_->Delete(doc_id, idx);
  }
#endif  // BUILD_GPU

  if (profile_->Update(fields_profile, doc_id) != 0) {
    LOG(ERROR) << "profile update error";
    return -1;
  }

#ifndef BUILD_GPU
  for (size_t i = 0; i < fields_profile.size(); ++i) {
    struct Field &field = fields_profile[i];
    int idx = profile_->GetAttrIdx(field.name);
    field_range_index_->Add(doc_id, idx);
  }
#endif  // BUILD_GPU

#ifdef DEBUG
  LOG(INFO) << "update success! key=" << key;
#endif
  return 0;
}

int GammaEngine::Delete(long key) {
  int docid = -1, ret = 0;
  ret = profile_->GetDocIDByKey(key, docid);
  if (ret != 0 || docid < 0) return -1;

  if (bitmap::test(docids_bitmap_, docid)) {
    return ret;
  }
  ++delete_num_;
  bitmap::set(docids_bitmap_, docid);

  vec_manager_->Delete(docid);

  return ret;
}

int GammaEngine::DelDocByQuery(Request &request) {
#ifdef DEBUG
  // LOG(INFO) << "delete by query request:" << RequestToString(request);
#endif

#ifndef BUILD_GPU
  std::vector<struct RangeFilter> &range_filters = request.RangeFilters();

  if (range_filters.size() <= 0) {
    LOG(ERROR) << "no range filter";
    return 1;
  }
  MultiRangeQueryResults range_query_result;  // Note its scope

  std::vector<FilterInfo> filters;
  filters.resize(range_filters.size());
  int idx = 0;

  for (size_t i = 0; i < range_filters.size(); ++i) {
    struct RangeFilter &range_filter = range_filters[i];

    filters[idx].field = profile_->GetAttrIdx(range_filter.field);
    filters[idx].lower_value = range_filter.lower_value;
    filters[idx].upper_value = range_filter.upper_value;

    ++idx;
  }

  int retval = field_range_index_->Search(filters, &range_query_result);
  if (retval == 0) {
    LOG(ERROR) << "numeric index search error, ret=" << retval;
    return 1;
  }

  std::vector<int> doc_ids = range_query_result.ToDocs();
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    int docid = doc_ids[i];
    if (bitmap::test(docids_bitmap_, docid)) {
      continue;
    }
    ++delete_num_;
    bitmap::set(docids_bitmap_, docid);
  }
#endif  // BUILD_GPU
  return 0;
}

int GammaEngine::GetDoc(long id, Doc &doc) {
  int docid = -1, ret = 0;
  ret = profile_->GetDocIDByKey(id, docid);
  if (ret != 0 || docid < 0) {
    LOG(INFO) << "GetDocIDbyKey [" << id << "] error!";
    return -1;
  }

  if (bitmap::test(docids_bitmap_, docid)) {
    LOG(INFO) << "docid [" << docid << "] is deleted! key=" << id;
    return -1;
  }
  std::vector<std::string> index_names;
  vec_manager_->VectorNames(index_names);

  std::map<std::string, enum DataType> attr_type_map;
  profile_->GetAttrType(attr_type_map);
  int i = 0;
  for (const auto &it : attr_type_map) {
    const string &attr = it.first;
    struct Field field;
    profile_->GetFieldInfo(docid, attr, field);
    doc.AddField(field);
    ++i;
  }

  profile_->GetDocInfo(docid, doc);

  std::vector<std::pair<std::string, int>> vec_fields_ids;
  for (size_t i = 0; i < index_names.size(); ++i) {
    vec_fields_ids.emplace_back(std::make_pair(index_names[i], docid));
  }

  std::vector<std::string> vec;
  ret = vec_manager_->GetVector(vec_fields_ids, vec, true);
  if (ret == 0 && vec.size() == vec_fields_ids.size()) {
    for (size_t i = 0; i < index_names.size(); ++i) {
      struct Field field;
      field.name = index_names[i];
      field.value = vec[i];
      doc.AddField(field);
    }
  }
  return 0;
}

int GammaEngine::BuildIndex() {
  if (vec_manager_->Indexing() != 0) {
    LOG(ERROR) << "Create index failed!";
    return -1;
  }

  if (b_running_) {
    return 0;
  }

  b_running_ = true;
  LOG(INFO) << "vector manager indexing success!";
  auto func_indexing = std::bind(&GammaEngine::Indexing, this);
  std::thread t(func_indexing);
  t.detach();
  return 0;
}

int GammaEngine::Indexing() {
  int ret = 0;
  bool has_error = false;
  while (b_running_) {
    if (has_error) {
      usleep(5000 * 1000);  // sleep 5000ms
      continue;
    }
    int add_ret = vec_manager_->AddRTVecsToIndex();
    if (add_ret != 0) {
      has_error = true;
      LOG(ERROR) << "Add real time vectors to index error!";
      continue;
    }
    index_status_ = IndexStatus::INDEXED;
    usleep(1000 * 1000);  // sleep 5000ms
  }
  running_cv_.notify_one();
  LOG(INFO) << "Build index exited!";
  return ret;
}

int GammaEngine::BuildFieldIndex() {
  b_field_running_ = true;

  std::map<std::string, enum DataType> attr_type_map;
  profile_->GetAttrType(attr_type_map);
  int field_num = attr_type_map.size();

  while (b_field_running_) {
    if (b_loading_) {
      usleep(5000 * 1000);  // sleep 5000ms
      continue;
    }
    int lastest_num = max_docid_;

#pragma omp parallel for
    for (int i = 0; i < field_num; ++i) {
      for (int j = indexed_field_num_; j < lastest_num; ++j) {
        field_range_index_->Add(j, i);
      }
    }

    indexed_field_num_ = lastest_num;
    usleep(5000 * 1000);  // sleep 5000ms
  }
  running_field_cv_.notify_one();
  LOG(INFO) << "Build field index exited!";
  return 0;
}

int GammaEngine::GetDocsNum() { return max_docid_ - delete_num_; }

void GammaEngine::GetIndexStatus(EngineStatus &engine_status) {
  engine_status.SetIndexStatus(index_status_);

  long profile_mem_bytes = profile_->GetMemoryBytes();
  long vec_mem_bytes, index_mem_bytes;
  vec_manager_->GetTotalMemBytes(index_mem_bytes, vec_mem_bytes);

  long dense_b = 0, sparse_b = 0, total_mem_b = 0;
#ifndef BUILD_GPU

  total_mem_b += field_range_index_->MemorySize(dense_b, sparse_b);
  // long total_mem_kb = total_mem_b / 1024;
  // long total_mem_mb = total_mem_kb / 1024;
  // LOG(INFO) << "Field range memory [" << total_mem_kb << "]kb, ["
  //           << total_mem_mb << "]MB, dense [" << dense_b / 1024 / 1024
  //           << "]MB sparse [" << sparse_b / 1024 / 1024
  //           << "]MB, indexed_field_num_ [" << indexed_field_num_ << "]";

  long total_mem_bytes = profile_mem_bytes + vec_mem_bytes + index_mem_bytes +
                         bitmap_bytes_size_ + total_mem_b;
#else   // BUILD_GPU
  long total_mem_bytes =
      profile_mem_bytes + vec_mem_bytes + index_mem_bytes + bitmap_bytes_size_;
#endif  // BUILD_GPU

  engine_status.SetProfileMem(profile_mem_bytes);
  engine_status.SetIndexMem(index_mem_bytes);
  engine_status.SetVectorMem(vec_mem_bytes);
  engine_status.SetFieldRangeMem(total_mem_b);
  engine_status.SetBitmapMem(bitmap_bytes_size_);
}

int GammaEngine::Dump() {
  int max_docid = max_docid_ - 1;
  if (max_docid <= dump_docid_) {
    LOG(INFO) << "No fresh doc, cannot dump.";
    return 0;
  }

  if (!utils::isFolderExist(dump_path_.c_str())) {
    mkdir(dump_path_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  std::time_t t = std::time(nullptr);
  char tm_str[100];
  std::strftime(tm_str, sizeof(tm_str), date_time_format_.c_str(),
                std::localtime(&t));

  string path = dump_path_ + "/" + tm_str;
  if (!utils::isFolderExist(path.c_str())) {
    mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  }

  const string dumping_file_name = path + "/dumping";
  std::ofstream f_dumping;
  f_dumping.open(dumping_file_name);
  if (!f_dumping.is_open()) {
    LOG(ERROR) << "Cannot create file " << dumping_file_name;
    return -1;
  }
  f_dumping << "start_docid " << dump_docid_ << std::endl;
  f_dumping << "end_docid " << max_docid << std::endl;
  f_dumping.close();

  int ret = profile_->Dump(path, dump_docid_, max_docid);
  if (ret != 0) {
    LOG(ERROR) << "dump profile error, ret=" << ret;
    return -1;
  }
  ret = vec_manager_->Dump(path, dump_docid_, max_docid);
  if (ret != 0) {
    LOG(ERROR) << "dump vector error, ret=" << ret;
    return -1;
  }

  const string bp_name = path + "/" + "bitmap";
  FILE *fp_output = fopen(bp_name.c_str(), "wb");
  if (fp_output == nullptr) {
    LOG(ERROR) << "Cannot write file " << bp_name;
    return -1;
  }

  fwrite((void *)(docids_bitmap_), sizeof(char), bitmap_bytes_size_, fp_output);
  fclose(fp_output);

  remove(last_bitmap_filename_.c_str());
  last_bitmap_filename_ = bp_name;

  dump_docid_ = max_docid + 1;

  const string dump_done_file_name = path + "/dump.done";
  if (rename(dumping_file_name.c_str(), dump_done_file_name.c_str())) {
    LOG(ERROR) << "rename " << dumping_file_name << " to "
               << dump_done_file_name << " error: " << strerror(errno);
    return -1;
  }

  LOG(INFO) << "Dumped to [" << path << "], next dump docid [" << dump_docid_
            << "]";
  return ret;
}

int GammaEngine::CreateTableFromLocal(std::string &table_name) {
  std::vector<string> file_paths = utils::ls(index_root_path_);
  for (string &file_path : file_paths) {
    std::string::size_type pos = file_path.rfind(".schema");
    if (pos == file_path.size() - 7) {
      std::string::size_type begin = file_path.rfind('/');
      assert(begin != std::string::npos);
      begin += 1;
      table_name = file_path.substr(begin, pos - begin);
      LOG(INFO) << "local table name=" << table_name;
      TableIO tio(file_path);
      Table table;
      if (tio.Read(table_name, table)) {
        LOG(ERROR) << "read table schema error, path=" << file_path;
        return -1;
      }

      if (CreateTable(table)) {
        LOG(ERROR) << "create table error when loading";
        return -1;
      }
      return 0;
    }
  }
  return -1;
}

int GammaEngine::Load() {
  b_loading_ = true;
  if (!created_table_) {
    string table_name;
    if (CreateTableFromLocal(table_name)) {
      LOG(ERROR) << "create table from local error";
      return -1;
    }
    LOG(INFO) << "create table from local success, table name=" << table_name;
  }

  std::map<std::time_t, string> folders_map;
  std::vector<std::time_t> folders_tm;
  std::vector<string> folders = utils::ls_folder(dump_path_);
  for (const string &folder_name : folders) {
    struct tm result;
    strptime(folder_name.c_str(), date_time_format_.c_str(), &result);

    std::time_t t = std::mktime(&result);
    folders_tm.push_back(t);
    folders_map.insert(std::make_pair(t, folder_name));
  }

  std::sort(folders_tm.begin(), folders_tm.end());
  folders.clear();
  string not_done_folder = "";
  for (const std::time_t t : folders_tm) {
    const string folder_path = dump_path_ + "/" + folders_map[t];
    const string done_file = folder_path + "/dump.done";
    if (utils::get_file_size(done_file.c_str()) < 0) {
      LOG(ERROR) << "dump.done cannot be found in [" << folder_path << "]";
      not_done_folder = folder_path;
      break;
    }
    folders.push_back(dump_path_ + "/" + folders_map[t]);
  }

  if (folders_tm.size() == 0) {
    LOG(INFO) << "no folder is found, skip loading!";
    return 0;
  }

  // there is only one folder which is not done
  if (not_done_folder != "") {
    int ret = utils::move_dir(not_done_folder.c_str(),
                              dump_backup_path_.c_str(), true);
    LOG(INFO) << "move " << not_done_folder << " to " << dump_backup_path_
              << ", ret=" << ret;
  }

  int ret = 0;
  if (folders.size() > 0) {
    ret = profile_->Load(folders, max_docid_);
    if (ret != 0) {
      LOG(ERROR) << "load profile error, ret=" << ret;
      return -1;
    }
    // load bitmap
    if (docids_bitmap_ == nullptr) {
      LOG(ERROR) << "docid bitmap is not initilized";
      return -1;
    }
    string bitmap_file_name = folders[folders.size() - 1] + "/bitmap";
    FILE *fp_bm = fopen(bitmap_file_name.c_str(), "rb");
    if (fp_bm == nullptr) {
      LOG(ERROR) << "Cannot open file " << bitmap_file_name;
      return -1;
    }
    long bm_file_size = utils::get_file_size(bitmap_file_name.c_str());
    if (bm_file_size > bitmap_bytes_size_) {
      LOG(ERROR) << "bitmap file size=" << bm_file_size
                 << " > allocated bitmap bytes size=" << bitmap_bytes_size_
                 << ", max doc size=" << max_doc_size_;
      fclose(fp_bm);
      return -1;
    }
    fread((void *)(docids_bitmap_), sizeof(char), bm_file_size, fp_bm);
    fclose(fp_bm);

    delete_num_ = 0;
    for (int i = 0; i < max_doc_size_; ++i) {
      if (bitmap::test(docids_bitmap_, i)) {
        ++delete_num_;
      }
    }
  }

  ret = vec_manager_->Load(folders, max_docid_);
  if (ret != 0) {
    LOG(ERROR) << "load vector error, ret=" << ret;
    return -1;
  }

  dump_docid_ = max_docid_;

  string last_folder = folders.size() > 0 ? folders[folders.size() - 1] : "";
  LOG(INFO) << "load engine success! max docid=" << max_docid_
            << ", last folder=" << last_folder;
  b_loading_ = false;
  return 0;
}

int GammaEngine::AddNumIndexFields() {
  int retvals = 0;
  std::map<std::string, enum DataType> attr_type;
  retvals = profile_->GetAttrType(attr_type);

  std::map<std::string, bool> attr_index;
  retvals = profile_->GetAttrIsIndex(attr_index);
  for (const auto &it : attr_type) {
    string field_name = it.first;
    const auto &attr_index_it = attr_index.find(field_name);
    if (attr_index_it == attr_index.end()) {
      LOG(ERROR) << "Cannot find field [" << field_name << "]";
      continue;
    }
    bool is_index = attr_index_it->second;
    if (not is_index) {
      continue;
    }
    int field_idx = profile_->GetAttrIdx(field_name);
    LOG(INFO) << "Add range field [" << field_name << "]";
    field_range_index_->AddField(field_idx, it.second);
  }
  return retvals;
}

int GammaEngine::PackResults(const GammaResult *gamma_results,
                             Response &response_results, Request &request) {
  for (int i = 0; i < request.ReqNum(); ++i) {
    struct SearchResult result;
    result.total = gamma_results[i].total;

    for (int j = 0; j < gamma_results[i].results_count; ++j) {
      VectorDoc *vec_doc = gamma_results[i].docs[j];
      struct ResultItem result_item;
      PackResultItem(vec_doc, request, result_item);
      result.result_items.emplace_back(result_item);
    }

    result.msg = "Success";
    result.result_code = SearchResultCode::SUCCESS;
    response_results.AddResults(result);
  }

  return 0;
}

int GammaEngine::PackResultItem(const VectorDoc *vec_doc, Request &request,
                                struct ResultItem &result_item) {
  result_item.score = vec_doc->score;

  Doc doc;
  int docid = vec_doc->docid;

  std::vector<std::string> &vec_fields = request.Fields();

  // add vector into result
  size_t fields_size = vec_fields.size();
  if (fields_size != 0) {
    std::vector<std::pair<string, int>> vec_fields_ids;
    std::vector<string> profile_fields;

    for (size_t i = 0; i < fields_size; ++i) {
      std::string &name = vec_fields[i];
      const auto index = vec_manager_->GetVectorIndex(name);
      if (index == nullptr) {
        profile_fields.push_back(name);
      } else {
        vec_fields_ids.emplace_back(std::make_pair(name, docid));
      }
    }

    std::vector<string> vec;
    int ret = vec_manager_->GetVector(vec_fields_ids, vec, true);

    int profile_fields_num = 0;

    if (profile_fields.size() == 0) {
      profile_fields_num = profile_->FieldsNum();

      profile_->GetDocInfo(docid, doc);
    } else {
      profile_fields_num = profile_fields.size();

      for (int i = 0; i < profile_fields_num; ++i) {
        struct Field field;
        profile_->GetFieldInfo(docid, profile_fields[i], field);
        doc.AddField(field);
      }
    }

    if (ret == 0 && vec.size() == vec_fields_ids.size()) {
      for (size_t i = 0; i < vec_fields_ids.size(); ++i) {
        const string &field_name = vec_fields_ids[i].first;
        result_item.names.emplace_back(field_name);
        result_item.values.emplace_back(vec[i]);
      }
    } else {
      // get vector error
      // TODO : release extra field
      ;
    }
  } else {
    profile_->GetDocInfo(docid, doc);
  }

  std::vector<struct Field> &fields = doc.Fields();
  for (struct Field &field : fields) {
    result_item.names.emplace_back(field.name);
    result_item.values.emplace_back(field.value);
  }

  cJSON *extra_json = cJSON_CreateObject();
  cJSON *vec_result_json = cJSON_CreateArray();
  cJSON_AddItemToObject(extra_json, EXTRA_VECTOR_RESULT.c_str(),
                        vec_result_json);
  for (int i = 0; i < vec_doc->fields_len; ++i) {
    VectorDocField *vec_field = vec_doc->fields + i;
    cJSON *vec_field_json = cJSON_CreateObject();

    cJSON_AddStringToObject(vec_field_json, EXTRA_VECTOR_FIELD_NAME.c_str(),
                            vec_field->name.c_str());
    string source = string(vec_field->source, vec_field->source_len);
    cJSON_AddStringToObject(vec_field_json, EXTRA_VECTOR_FIELD_SOURCE.c_str(),
                            source.c_str());
    cJSON_AddNumberToObject(vec_field_json, EXTRA_VECTOR_FIELD_SCORE.c_str(),
                            vec_field->score);
    cJSON_AddItemToArray(vec_result_json, vec_field_json);
  }

  char *extra_data = cJSON_PrintUnformatted(extra_json);
  result_item.extra = std::string(extra_data, std::strlen(extra_data));
  free(extra_data);
  cJSON_Delete(extra_json);

  return 0;
}

}  // namespace tig_gamma
