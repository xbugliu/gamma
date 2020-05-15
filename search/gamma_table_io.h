/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "api_data/gamma_table.h"
#include "utils.h"

namespace tig_gamma {

class TableIO {
 public:
  TableIO(std::string &file_path);

  ~TableIO();

  int Write(Table &table);

  void WriteFieldInfos(Table &table);

  void WriteVectorInfos(Table &table);

  void WriteRetrievalType(Table &table);

  void WriteRetrievalParam(Table &table);

  int Read(std::string &name, Table &table);

  void ReadFieldInfos(Table &table);

  void ReadVectorInfos(Table &table);

  void ReadRetrievalType(Table &table);

  void ReadRetrievalParam(Table &table);

  utils::FileIO *fio;
};

}  // namespace tig_gamma