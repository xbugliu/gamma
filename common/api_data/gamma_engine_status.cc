/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "gamma_engine_status.h"

namespace tig_gamma {

EngineStatus::EngineStatus() { engine_status_ = nullptr; }

int EngineStatus::Serialize(char **out, int *out_len) {
  flatbuffers::FlatBufferBuilder builder;
  auto table = gamma_api::CreateEngineStatus(
      builder, index_status_, profile_mem_, index_mem_, vector_mem_,
      field_range_mem_, bitmap_mem_);
  builder.Finish(table);
  *out_len = builder.GetSize();
  *out = (char *)malloc(*out_len * sizeof(char));
  memcpy(*out, (char *)builder.GetBufferPointer(), *out_len);
  return 0;
}

void EngineStatus::Deserialize(const char *data, int len) {
  engine_status_ =
      const_cast<gamma_api::EngineStatus *>(gamma_api::GetEngineStatus(data));

  index_status_ = engine_status_->index_status();
  profile_mem_ = engine_status_->profile_mem();
  index_mem_ = engine_status_->index_mem();
  vector_mem_ = engine_status_->vector_mem();
  field_range_mem_ = engine_status_->field_range_mem();
  bitmap_mem_ = engine_status_->bitmap_mem();
}

int EngineStatus::IndexStatus() { return index_status_; }

void EngineStatus::SetIndexStatus(int index_status) {
  index_status_ = index_status;
}

long EngineStatus::ProfileMem() { return profile_mem_; }

void EngineStatus::SetProfileMem(long profile_mem) {
  profile_mem_ = profile_mem;
}

long EngineStatus::IndexMem() { return index_mem_; }

void EngineStatus::SetIndexMem(long index_mem) { index_mem_ = index_mem; }

long EngineStatus::VectorMem() { return vector_mem_; }

void EngineStatus::SetVectorMem(long vector_mem) { vector_mem_ = vector_mem; }

long EngineStatus::FieldRangeMem() { return field_range_mem_; }

void EngineStatus::SetFieldRangeMem(long field_range_mem) {
  field_range_mem_ = field_range_mem;
}

long EngineStatus::BitmapMem() { return bitmap_mem_; }

void EngineStatus::SetBitmapMem(long bitmap_mem) { bitmap_mem_ = bitmap_mem; }
}  // namespace tig_gamma