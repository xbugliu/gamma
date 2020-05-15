/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef PROFILE_H_
#define PROFILE_H_

#include <cuckoohash_map.hh>
#include <map>
#include <string>
#include <vector>

#include "api_data/gamma_doc.h"
#include "api_data/gamma_table.h"
#include "log.h"

#ifdef USE_BTREE
#include "threadskv10h.h"
#endif

#ifdef WITH_ROCKSDB
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#endif

namespace tig_gamma {

/** profile, support add, update, delete, dump and load.
 */
class Profile {
 public:
  explicit Profile(const int max_doc_size, const std::string &root_path);

  ~Profile();

  /** create table
   *
   * @param table  table definition
   * @return 0 if successed
   */
  int CreateTable(Table &table);

  /** add a doc to table
   *
   * @param doc     doc to add
   * @param doc_idx doc index number
   * @return 0 if successed
   */
  int Add(const std::vector<struct Field> &fields, int doc_id,
          bool is_existed = false);

  /** update a doc
   *
   * @param doc     doc to update
   * @param doc_idx doc index number
   * @return 0 if successed
   */
  int Update(const std::vector<struct Field> &fields, int doc_id);

  /** get docid by key
   *
   * @param key key to get
   * @param doc_id output, the docid to key
   * @return 0 if successed, -1 key not found
   */
  int GetDocIDByKey(long key, int &doc_id);

  /** dump datas to disk
   *
   * @return ResultCode
   */
  int Dump(const std::string &path, int start_docid, int end_docid);

  long GetMemoryBytes();

  int GetDocInfo(long id, Doc &doc);
  int GetDocInfo(const int docid, Doc &doc);

  void GetFieldInfo(const int docid, const std::string &field_name,
                    struct Field &field);

  template <typename T>
  bool GetField(const int docid, const int field_id, T &value) const {
    if ((docid < 0) or (field_id < 0 || field_id >= field_num_)) return false;

    size_t offset = (uint64_t)docid * item_length_ + idx_attr_offset_[field_id];
    memcpy(&value, mem_ + offset, sizeof(T));
    return true;
  }

  template <typename T>
  void GetField(int docid, const std::string &field, T &value) const {
    const auto &iter = attr_idx_map_.find(field);
    if (iter == attr_idx_map_.end()) {
      return;
    }
    GetField<T>(docid, iter->second, value);
  }

  int GetFieldString(int docid, const std::string &field, char **value) const;

  int GetFieldString(int docid, int field_id, char **value) const;

  int GetFieldRawValue(int docid, int field_id, unsigned char **value,
                       int &data_len);

  int GetFieldType(const std::string &field, enum DataType &type);

  int GetAttrType(std::map<std::string, enum DataType> &attr_type_map);

  int GetAttrIsIndex(std::map<std::string, bool> &attr_is_index_map);

  int GetAttrIdx(const std::string &field) const;

  int Load(const std::vector<std::string> &folders, int &doc_num);

  int FieldsNum() { return attrs_.size(); };

 private:
  int FTypeSize(enum DataType fType);

  void SetFieldValue(int docid, const std::string &field, const char *value,
                     uint16_t len);

  int AddField(const std::string &name, DataType ftype, bool is_index);

  void ToRowKey(int id, std::string &key) const;

  int GetRawDoc(int docid, std::vector<char> &raw_doc);

  int PutToDB(int docid);

  std::string name_;   // table name
  int item_length_;    // every doc item length
  uint8_t field_num_;  // field number
  int key_idx_;        // key postion

  std::map<int, std::string> idx_attr_map_;
  std::map<std::string, int> attr_idx_map_;
  std::map<std::string, DataType> attr_type_map_;
  std::map<std::string, bool> attr_is_index_map_;
  std::vector<int> idx_attr_offset_;
  std::vector<DataType> attrs_;
  cuckoohash_map<long, int> item_to_docid_;

  char *mem_;
  char *str_mem_;
  uint64_t max_profile_size_;
  uint64_t max_str_size_;
  uint64_t str_offset_;

  bool table_created_;
#ifdef WITH_ROCKSDB
  rocksdb::DB *db_;
#endif
  std::string db_path_;

#ifdef USE_BTREE
  BtMgr *main_mgr_;
  BtMgr *cache_mgr_;
#endif
};

}  // namespace tig_gamma

#endif
