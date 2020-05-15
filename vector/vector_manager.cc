/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "vector_manager.h"

#include "gamma_index_factory.h"
#include "raw_vector_factory.h"
#include "utils.h"

namespace tig_gamma {

static bool InnerProductCmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score > b->score;
}

static bool L2Cmp(const VectorDoc *a, const VectorDoc *b) {
  return a->score < b->score;
}

VectorManager::VectorManager(const RetrievalModel &model,
                             const VectorStorageType &store_type,
                             const char *docids_bitmap, int max_doc_size,
                             const std::string &root_path,
                             GammaCounters *counters)
    : default_model_(model),
      default_store_type_(store_type),
      docids_bitmap_(docids_bitmap),
      max_doc_size_(max_doc_size),
      root_path_(root_path),
      gamma_counters_(counters) {
  table_created_ = false;
  retrieval_param_ = nullptr;
}

VectorManager::~VectorManager() { Close(); }

int VectorManager::CreateVectorTable(Table &table) {
  if (table_created_) return -1;

  std::string &retrieval_param = table.RetrievalParam();
  retrieval_param_ = new RetrievalParams();
  retrieval_param_->Parse(table.RetrievalParam().c_str());

  std::map<string, int> vec_dups;

  std::vector<struct VectorInfo> &vectors_infos = table.VectorInfos();

  for (struct VectorInfo &vectors_info : vectors_infos) {
    std::string &name = vectors_info.name;
    auto it = vec_dups.find(name);
    if (it == vec_dups.end()) {
      vec_dups[name] = 1;
    } else {
      ++vec_dups[name];
    }
  }

  for (size_t i = 0; i < vectors_infos.size(); i++) {
    struct VectorInfo vector_info = vectors_infos[i];
    std::string &vec_name = vector_info.name;
    int dimension = vector_info.dimension;

    std::string &store_type_str = vector_info.store_type;

    VectorStorageType store_type = default_store_type_;
    if (store_type_str != "") {
      if (!strcasecmp("Mmap", store_type_str.c_str())) {
        store_type = VectorStorageType::Mmap;
#ifdef WITH_ROCKSDB
      } else if (!strcasecmp("RocksDB", store_type_str.c_str())) {
        store_type = VectorStorageType::RocksDB;
#endif  // WITH_ROCKSDB
      } else {
        LOG(WARNING) << "NO support for store type " << store_type_str;
        return -1;
      }
    }

    std::string &store_param = vector_info.store_param;

    std::string &retrieval_type_str = table.RetrievalType();

    RetrievalModel model = default_model_;
    if (!strcasecmp("IVFPQ", retrieval_type_str.c_str())) {
      model = RetrievalModel::IVFPQ;
    } else if (!strcasecmp("GPU", retrieval_type_str.c_str())) {
      model = RetrievalModel::GPU_IVFPQ;
    } else if (!strcasecmp("BINARYIVF", retrieval_type_str.c_str())) {
      model = RetrievalModel::BINARYIVF;
    } else if (!strcasecmp("HNSW", retrieval_type_str.c_str())) {
      model = RetrievalModel::HNSW;
    } else if (!strcasecmp("FLAT", retrieval_type_str.c_str())) {
      model = RetrievalModel::FLAT;
    } else {
      LOG(ERROR) << "NO support for retrieval type " << retrieval_type_str;
      return -1;
    }

    if (model == RetrievalModel::BINARYIVF) {
      RawVector<uint8_t> *vec = RawVectorFactory::CreateBinary(
          store_type, vec_name, dimension / 8, max_doc_size_, root_path_,
          store_param);
      if (vec == nullptr) {
        LOG(ERROR) << "create raw vector error";
        return -1;
      }
      bool has_source = vector_info.has_source;
      bool multi_vids = vec_dups[vec_name] > 1 ? true : false;
      int ret = vec->Init(has_source, multi_vids);
      if (ret != 0) {
        LOG(ERROR) << "Raw vector " << vec_name << " init error, code [" << ret
                   << "]!";
        return -1;
      }

      StartFlushingIfNeed<uint8_t>(vec);
      raw_binary_vectors_[vec_name] = vec;

      GammaIndex *index =
          GammaIndexFactory::CreateBinary(model, dimension, docids_bitmap_, vec,
                                          retrieval_param, gamma_counters_);
      if (index == nullptr) {
        LOG(ERROR) << "create gamma index " << vec_name << " error!";
        return -1;
      }
      index->SetRawVectorBinary(vec);
      if (vector_info.is_index == false) {
        LOG(INFO) << vec_name << " need not to indexed!";
        continue;
      }

      vector_indexes_[vec_name] = index;
    } else {
      RawVector<float> *vec =
          RawVectorFactory::Create(store_type, vec_name, dimension,
                                   max_doc_size_, root_path_, store_param);
      if (vec == nullptr) {
        LOG(ERROR) << "create raw vector error";
        return -1;
      }
      bool has_source = vector_info.has_source;
      bool multi_vids = vec_dups[vec_name] > 1 ? true : false;
      int ret = vec->Init(has_source, multi_vids);
      if (ret != 0) {
        LOG(ERROR) << "Raw vector " << vec_name << " init error, code [" << ret
                   << "]!";
        return -1;
      }

      StartFlushingIfNeed<float>(vec);
      raw_vectors_[vec_name] = vec;

      GammaIndex *index =
          GammaIndexFactory::Create(model, dimension, docids_bitmap_, vec,
                                    retrieval_param, gamma_counters_);
      if (index == nullptr) {
        LOG(ERROR) << "create gamma index " << vec_name << " error!";
        return -1;
      }
      index->SetRawVectorFloat(vec);
      if (vector_info.is_index == false) {
        LOG(INFO) << vec_name << " need not to indexed!";
        continue;
      }

      vector_indexes_[vec_name] = index;
    }
  }
  table_created_ = true;
  return 0;
}

int VectorManager::AddToStore(int docid, std::vector<struct Field> &fields) {
  for (unsigned int i = 0; i < fields.size(); i++) {
    std::string &name = fields[i].name;
    if (raw_vectors_.find(name) == raw_vectors_.end()) {
      // LOG(ERROR) << "Cannot find raw vector [" << name << "]";
      continue;
    }
    raw_vectors_[name]->Add(docid, fields[i]);
  }

  for (unsigned int i = 0; i < fields.size(); i++) {
    std::string name = fields[i].name;
    if (raw_binary_vectors_.find(name) == raw_binary_vectors_.end()) {
      // LOG(ERROR) << "Cannot find raw vector [" << name << "]";
      continue;
    }
    raw_binary_vectors_[name]->Add(docid, fields[i]);
  }
  return 0;
}

int VectorManager::Update(int docid, std::vector<Field> &fields) {
  for (unsigned int i = 0; i < fields.size(); i++) {
    string &name = fields[i].name;
    auto it = raw_vectors_.find(name);
    if (it == raw_vectors_.end()) {
      LOG(ERROR) << "Cannot find raw vector [" << name << "]";
      return -1;
    }
    RawVector<float> *raw_vector = it->second;
    if ((size_t)raw_vector->GetDimension() !=
        fields[i].value.size() / sizeof(float)) {
      LOG(ERROR) << "invalid field value len=" << fields[i].value.size()
                 << ", dimension=" << raw_vector->GetDimension();
      return -1;
    }

    return raw_vector->Update(docid, fields[i]);
  }
  return 0;
}

int VectorManager::Delete(int docid) {
  for (const auto &iter : vector_indexes_) {
    if (0 != iter.second->Delete(docid)) {
      LOG(ERROR) << "delete index from" << iter.first
                 << " failed! docid=" << docid;
      return -1;
    }
  }
  return 0;
}

int VectorManager::Indexing() {
  int ret = 0;
  for (const auto &iter : vector_indexes_) {
    if (0 != iter.second->Indexing()) {
      ret = -1;
      LOG(ERROR) << "vector table " << iter.first << " indexing failed!";
    }
  }
  return ret;
}

int VectorManager::AddRTVecsToIndex() {
  int ret = 0;
  for (const auto &iter : vector_indexes_) {
    if (0 != iter.second->AddRTVecsToIndex()) {
      ret = -1;
      LOG(ERROR) << "vector table " << iter.first
                 << " add real time vectors failed!";
    }
  }
  return ret;
}

int VectorManager::Search(GammaQuery &query, GammaResult *results) {
  int ret = 0, n = 0;

  size_t vec_num = query.vec_query.size();
  VectorResult all_vector_results[vec_num];

  query.condition->sort_by_docid = vec_num > 1 ? true : false;
  query.condition->metric_type =
      static_cast<DistanceMetricType>(retrieval_param_->metric_type);
  std::string vec_names[vec_num];
  for (size_t i = 0; i < vec_num; i++) {
    struct VectorQuery &vec_query = query.vec_query[i];

    std::string &name = vec_query.name;
    vec_names[i] = name;
    std::map<std::string, GammaIndex *>::iterator iter =
        vector_indexes_.find(name);
    if (iter == vector_indexes_.end()) {
      LOG(ERROR) << "Query name " << name
                 << " not exist in created vector table";
      return -1;
    }

    GammaIndex *index = iter->second;
    int d = 0;
    if (index->raw_vec_binary_ != nullptr) {
      d = index->raw_vec_binary_->GetDimension();
      n = vec_query.value.size() / (sizeof(uint8_t) * d);
    } else {
      d = index->raw_vec_->GetDimension();
      n = vec_query.value.size() / (sizeof(float) * d);
    }

    if (!all_vector_results[i].init(n, query.condition->topn)) {
      LOG(ERROR) << "Query name " << name << "init vector result error";
      return -2;
    }

    query.condition->min_dist = vec_query.min_score;
    query.condition->max_dist = vec_query.max_score;
    int ret_vec =
        index->Search(vec_query, query.condition, all_vector_results[i]);
    if (ret_vec != 0) {
      ret = ret_vec;
    }
#ifdef PERFORMANCE_TESTING
    std::string msg;
    msg += "search " + std::to_string(i);
    query.condition->Perf(msg);
#endif
  }

  if (query.condition->sort_by_docid) {
    for (int i = 0; i < n; i++) {
      int start_docid = 0, common_idx = 0;
      size_t common_docid_count = 0;
      double score = 0;
      bool has_common_docid = true;
      if (!results[i].init(query.condition->topn, vec_names, vec_num)) {
        LOG(ERROR) << "init gamma result(sort by docid) error, topn="
                   << query.condition->topn << ", vector number=" << vec_num;
        return -3;
      }
      while (start_docid < INT_MAX) {
        for (size_t j = 0; j < vec_num; j++) {
          float vec_dist = 0;
          char *source = nullptr;
          int source_len = 0;
          int cur_docid = all_vector_results[j].seek(i, start_docid, vec_dist,
                                                     source, source_len);
          if (cur_docid == start_docid) {
            common_docid_count++;
            double field_score = query.vec_query[j].has_boost == 1
                                     ? (vec_dist * query.vec_query[j].boost)
                                     : vec_dist;
            score += field_score;
            results[i].docs[common_idx]->fields[j].score = field_score;
            results[i].docs[common_idx]->fields[j].source = source;
            results[i].docs[common_idx]->fields[j].source_len = source_len;
            if (common_docid_count == vec_num) {
              results[i].docs[common_idx]->docid = start_docid;
              results[i].docs[common_idx++]->score = score;
              results[i].total = all_vector_results[j].total[i] > 0
                                     ? all_vector_results[j].total[i]
                                     : results[i].total;

              start_docid++;
              common_docid_count = 0;
              score = 0;
            }
          } else if (cur_docid > start_docid) {
            common_docid_count = 0;
            start_docid = cur_docid;
            score = 0;
          } else {
            has_common_docid = false;
            break;
          }
        }
        if (!has_common_docid) break;
      }
      results[i].results_count = common_idx;
      if (query.condition->multi_vector_rank) {
        switch (query.condition->metric_type) {
          case DistanceMetricType::InnerProduct:
            std::sort(results[i].docs, results[i].docs + common_idx,
                      InnerProductCmp);
            break;
          case DistanceMetricType::L2:
            std::sort(results[i].docs, results[i].docs + common_idx, L2Cmp);
            break;
          default:
            LOG(ERROR) << "invalid metric_type="
                       << (int)query.condition->metric_type;
        }
      }
    }
  } else {
    for (int i = 0; i < n; i++) {
      // double score = 0;
      if (!results[i].init(query.condition->topn, vec_names, vec_num)) {
        LOG(ERROR) << "init gamma result error, topn=" << query.condition->topn
                   << ", vector number=" << vec_num;
        return -4;
      }
      results[i].total = all_vector_results[0].total[i] > 0
                             ? all_vector_results[0].total[i]
                             : results[i].total;
      int pos = 0, topn = all_vector_results[0].topn;
      for (int j = 0; j < topn; j++) {
        int real_pos = i * topn + j;
        if (all_vector_results[0].docids[real_pos] == -1) continue;
        results[i].docs[pos]->docid = all_vector_results[0].docids[real_pos];

        results[i].docs[pos]->fields[0].source =
            all_vector_results[0].sources[real_pos];
        results[i].docs[pos]->fields[0].source_len =
            all_vector_results[0].source_lens[real_pos];

        double score = all_vector_results[0].dists[real_pos];

        score = query.vec_query[0].has_boost == 1
                    ? (score * query.vec_query[0].boost)
                    : score;

        results[i].docs[pos]->fields[0].score = score;
        results[i].docs[pos]->score = score;
        pos++;
      }
      results[i].results_count = pos;
    }
  }

#ifdef PERFORMANCE_TESTING
  query.condition->Perf("merge result");
#endif
  return ret;
}

int VectorManager::GetVector(
    const std::vector<std::pair<string, int>> &fields_ids,
    std::vector<string> &vec, bool is_bytearray) {
  for (const auto &pair : fields_ids) {
    const string &field = pair.first;
    const int id = pair.second;
    std::map<std::string, GammaIndex *>::iterator iter =
        vector_indexes_.find(field);
    if (iter == vector_indexes_.end()) {
      continue;
    }
    GammaIndex *gamma_index = iter->second;
    if (gamma_index->raw_vec_ != nullptr) {
      RawVector<float> *raw_vec = gamma_index->raw_vec_;
      if (raw_vec == nullptr) {
        LOG(ERROR) << "raw_vec is null!";
        return -1;
      }
      int vid = raw_vec->vid_mgr_->GetFirstVID(id);

      char *source = nullptr;
      int len = -1;
      int ret = raw_vec->GetSource(vid, source, len);

      if (ret != 0 || len < 0) {
        LOG(ERROR) << "Get source failed!";
        return -1;
      }

      ScopeVector<float> scope_vec;
      raw_vec->GetVector(vid, scope_vec);
      const float *feature = scope_vec.Get();
      string str_vec;
      if (is_bytearray) {
        int d = raw_vec->GetDimension();
        int d_byte = d * sizeof(float);

        char feat_source[sizeof(d) + d_byte + len];

        memcpy((void *)feat_source, &d_byte, sizeof(int));
        int cur = sizeof(d_byte);

        memcpy((void *)(feat_source + cur), feature, d_byte);
        cur += d_byte;

        memcpy((void *)(feat_source + cur), source, len);

        str_vec =
            string((char *)feat_source, sizeof(unsigned int) + d_byte + len);
      } else {
        for (int i = 0; i < raw_vec->GetDimension(); ++i) {
          str_vec += std::to_string(feature[i]) + ",";
        }
        str_vec.pop_back();
      }
      vec.emplace_back(std::move(str_vec));
    } else {
      RawVector<uint8_t> *raw_vec = gamma_index->raw_vec_binary_;
      if (raw_vec == nullptr) {
        LOG(ERROR) << "raw_vec is null!";
        return -1;
      }

      int vid = raw_vec->vid_mgr_->GetFirstVID(id);

      char *source = nullptr;
      int len = -1;
      int ret = raw_vec->GetSource(vid, source, len);

      if (ret != 0 || len < 0) {
        LOG(ERROR) << "Get source failed!";
        return -1;
      }

      ScopeVector<uint8_t> scope_vec;
      raw_vec->GetVector(vid, scope_vec);
      const uint8_t *feature = scope_vec.Get();
      string str_vec;
      if (is_bytearray) {
        int d = raw_vec->GetDimension();
        int d_byte = d * sizeof(uint8_t);

        char feat_source[sizeof(d) + d_byte + len];

        memcpy((void *)feat_source, &d_byte, sizeof(int));
        int cur = sizeof(d_byte);

        memcpy((void *)(feat_source + cur), feature, d_byte);
        cur += d_byte;

        memcpy((void *)(feat_source + cur), source, len);

        str_vec =
            string((char *)feat_source, sizeof(unsigned int) + d_byte + len);
      } else {
        for (int i = 0; i < raw_vec->GetDimension(); ++i) {
          str_vec += std::to_string(feature[i]) + ",";
        }
        str_vec.pop_back();
      }
      vec.emplace_back(std::move(str_vec));
    }
  }
  return 0;
}

int VectorManager::Dump(const string &path, int dump_docid, int max_docid) {
  for (const auto &iter : vector_indexes_) {
    const string &vec_name = iter.first;
    GammaIndex *index = iter.second;

    auto it = raw_vectors_.find(vec_name);
    assert(it != raw_vectors_.end());
    int max_vid = it->second->vid_mgr_->GetLastVID(max_docid);
    int dump_num = index->Dump(path, max_vid);
    if (dump_num < 0) {
      LOG(ERROR) << "vector " << vec_name << " dump gamma index failed!";
      return -1;
    }
    LOG(INFO) << "vector " << vec_name << " dump gamma index success!";
  }

  for (const auto &iter : raw_vectors_) {
    const string &vec_name = iter.first;
    RawVector<float> *raw_vector = iter.second;
    int ret = raw_vector->Dump(path, dump_docid, max_docid);
    if (ret != 0) {
      LOG(ERROR) << "vector " << vec_name << " dump failed!";
      return -1;
    }
    LOG(INFO) << "vector " << vec_name << " dump success!";
  }

  for (const auto &iter : raw_binary_vectors_) {
    const string &vec_name = iter.first;
    RawVector<uint8_t> *raw_vector = iter.second;
    int ret = raw_vector->Dump(path, dump_docid, max_docid);
    if (ret != 0) {
      LOG(ERROR) << "vector " << vec_name << " dump failed!";
      return -1;
    }
    LOG(INFO) << "vector " << vec_name << " dump success!";
  }
  return 0;
}

int VectorManager::Load(const std::vector<std::string> &index_dirs,
                        int doc_num) {
  for (const auto &iter : raw_vectors_) {
    if (0 != iter.second->Load(index_dirs, doc_num)) {
      LOG(ERROR) << "vector [" << iter.first << "] load failed!";
      return -1;
    }
    LOG(INFO) << "vector [" << iter.first << "] load success!";
  }

  for (const auto &iter : raw_binary_vectors_) {
    if (0 != iter.second->Load(index_dirs, doc_num)) {
      LOG(ERROR) << "vector [" << iter.first << "] load failed!";
      return -1;
    }
    LOG(INFO) << "vector [" << iter.first << "] load success!";
  }

  if (index_dirs.size() > 0) {
    for (const auto &iter : vector_indexes_) {
      if (iter.second->Load(index_dirs) < 0) {
        LOG(ERROR) << "vector [" << iter.first << "] load gamma index failed!";
        return -1;
      } else {
        LOG(INFO) << "vector [" << iter.first << "] load gamma index success!";
      }
    }
  }

  return 0;
}

void VectorManager::Close() {
  for (const auto &iter : raw_vectors_) {
    if (iter.second != nullptr) {
      StopFlushingIfNeed(iter.second);
      delete iter.second;
    }
  }
  raw_vectors_.clear();
  LOG(INFO) << "Raw vector cleared.";

  for (const auto &iter : raw_binary_vectors_) {
    if (iter.second != nullptr) {
      StopFlushingIfNeed(iter.second);
      delete iter.second;
    }
  }
  raw_binary_vectors_.clear();

  for (const auto &iter : vector_indexes_) {
    if (iter.second != nullptr) {
      delete iter.second;
    }
  }
  vector_indexes_.clear();
  LOG(INFO) << "Vector indexes cleared.";

  if (retrieval_param_ != nullptr) {
    delete retrieval_param_;
    retrieval_param_ = nullptr;
  }
  LOG(INFO) << "VectorManager closed.";
}
}  // namespace tig_gamma
