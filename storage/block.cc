/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "block.h"

#include <unistd.h>

namespace tig_gamma {

Block::Block(int fd, int per_block_size, int length, uint32_t header_size,
             uint32_t seg_id, uint32_t seg_block_capacity)
    : fd_(fd),
      per_block_size_(per_block_size),
      item_length_(length),
      header_size_(header_size),
      seg_block_capacity_(seg_block_capacity),
      seg_id_(seg_id) {
  compressor_ = nullptr;
  size_ = 0;
  last_bid_in_disk_ = 0;
}

Block::~Block() {
  lru_cache_ = nullptr;
  compressor_ = nullptr;
}

void Block::Init(void *lru, Compressor *compressor) {
  lru_cache_ =
      (LRUCache<uint32_t, std::vector<uint8_t>, ReadFunParameter *> *)lru;
  compressor_ = compressor;
  InitSubclass();
}

int Block::Write(const uint8_t *value, int n_bytes, uint32_t start,
                 disk_io::AsyncWriter *disk_io) {
  size_ += n_bytes;
  WriteContent(value, n_bytes, start, disk_io);
  return 0;
}

static uint32_t WritenSize(int fd) {
  uint32_t size;
  pread(fd, &size, sizeof(size), sizeof(uint8_t) + sizeof(uint32_t));
  return size;
}

int Block::Read(uint8_t *value, uint32_t n_bytes, uint32_t start) {
  int read_num = 0;
  while (n_bytes) {
    int len = n_bytes;
    if (len > per_block_size_) len = per_block_size_;

    uint32_t block_id = start / per_block_size_;
    // uint32_t block_pos = block_pos_[block_id];
    uint32_t block_pos = block_id * per_block_size_;
    uint32_t block_offset = start % per_block_size_;

    if (len > per_block_size_ - block_offset)
      len = per_block_size_ - block_offset;

    if (last_bid_in_disk_ <= block_id) {
      uint32_t cur_size = WritenSize(fd_); 
      last_bid_in_disk_ = cur_size * item_length_ / per_block_size_;
    }
    if (last_bid_in_disk_ <= block_id) {
      ReadContent(value + read_num, len, block_pos + block_offset);
    } else {
      std::shared_ptr<std::vector<uint8_t>> block;
      uint32_t cache_bid = GetCacheBlockId(block_id);
      bool res = lru_cache_->Get(cache_bid, block);
      if (not res) {
        ReadFunParameter parameter;
        GetReadFunParameter(parameter, per_block_size_, block_pos);
        res = lru_cache_->SetOrGet(cache_bid, block, &parameter);
      }
      if (not res) {
        LOG(ERROR) << "Read block fails from disk_file, block_id[" << block_id
                   << "]";
        return -1;
      }
      memcpy(value + read_num, block->data() + block_offset, len);
    }

    start += len;
    read_num += len;
    n_bytes -= len;
  }
  return 0;
}

int Block::Update(const uint8_t *data, int n_bytes, uint32_t offset) {
  int res = SubclassUpdate(data, n_bytes, offset);
  if (res != 0) return res;

  while (n_bytes) {
    int len = n_bytes;
    if (len > per_block_size_) len = per_block_size_;

    uint32_t block_id = offset / per_block_size_;
    uint32_t block_offset = offset % per_block_size_;

    if (len > per_block_size_ - block_offset)
      len = per_block_size_ - block_offset;

    uint32_t cache_block_id = seg_id_ * seg_block_capacity_ + block_id;
    lru_cache_->Evict(cache_block_id);

    offset += len;
    n_bytes -= len;
  }
  return res;
}

void Block::SegmentIsFull() {
  ++last_bid_in_disk_;
}

int32_t Block::GetCacheBlockId(uint32_t block_id) {
  return seg_id_ * seg_block_capacity_ + block_id;
}

}  // namespace tig_gamma