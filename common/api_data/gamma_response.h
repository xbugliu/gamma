/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "gamma_common_data.h"
#include "gamma_raw_data.h"
#include "response_generated.h"

namespace tig_gamma {

enum class SearchResultCode : std::uint16_t {
  SUCCESS = 0,
  INDEX_NOT_TRAINED,
  SEARCH_ERROR
};

struct ResultItem {
  double score;
  std::vector<std::string> names;
  std::vector<std::string> values;
  std::string extra;
};

struct SearchResult {
  SearchResult() {
    total = 0;
  }
  int total;
  SearchResultCode result_code;
  std::string msg;
  std::vector<struct ResultItem> result_items;
};

class Response : public RawData {
 public:
  Response();

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  void AddResults(struct SearchResult &result);

  std::vector<struct SearchResult> &Results();

  void SetOnlineLogMessage(const std::string &msg);

 private:
  gamma_api::Response *response_;
  std::vector<struct SearchResult> results_;
  std::string online_log_message_;
};

}  // namespace tig_gamma