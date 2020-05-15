/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <vector>

#include "config_generated.h"
#include "gamma_raw_data.h"

namespace tig_gamma {

class Config : public RawData {
 public:
  Config() { config_ = nullptr; }

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  int MaxDocSize();

  void SetMaxDocSize(int max_doc_size);

  const std::string &Path();

  void SetPath(std::string &path);

  const std::string &LogDir();

  void SetLogDir(std::string &log_dir);

 private:
  gamma_api::Config *config_;

  std::string path_;
  std::string log_dir_;
  int max_doc_size_;
};

}  // namespace tig_gamma
