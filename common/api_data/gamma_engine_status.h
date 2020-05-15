/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "api_data/gamma_raw_data.h"
#include "engine_status_generated.h"

namespace tig_gamma {

class EngineStatus : public RawData {
 public:
  EngineStatus();

  virtual int Serialize(char **out, int *out_len);

  virtual void Deserialize(const char *data, int len);

  int IndexStatus();

  void SetIndexStatus(int index_status);

  long ProfileMem();

  void SetProfileMem(long profile_mem);

  long IndexMem();

  void SetIndexMem(long index_mem);

  long VectorMem();

  void SetVectorMem(long vector_mem);

  long FieldRangeMem();

  void SetFieldRangeMem(long field_range_mem);

  long BitmapMem();

  void SetBitmapMem(long bitmap_mem);

 private:
  gamma_api::EngineStatus *engine_status_;

  int index_status_;
  long profile_mem_;
  long index_mem_;
  long vector_mem_;
  long field_range_mem_;
  long bitmap_mem_;
};

}  // namespace tig_gamma