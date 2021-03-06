/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <stdint.h>
#include <unistd.h>
#include <vector>
#include <tbb/concurrent_vector.h>

#include "async_writer.h"
#include "lru_cache.h"
#include "compress/compressor_zfp.h"
#include "compress/compressor_zstd.h"

typedef uint32_t str_offset_t;
typedef uint16_t str_len_t;

namespace tig_gamma {

// struct ReadFunParameter{
//   int fd;
//   uint32_t len;
//   uint32_t offset;
// };

enum class BlockType : uint8_t {TableBlockType = 0, StringBlockType, VectorBlockType};

class Block {
 public:
  Block(int fd, int per_block_size, int length, uint32_t header_size,
        uint32_t seg_id, uint32_t seg_block_capacity);

  virtual ~Block();

  void Init(void *lru, Compressor *compressor = nullptr);

  int Write(const uint8_t *data, int len, uint32_t offset,
            disk_io::AsyncWriter *disk_io);

  int Read(uint8_t *value, uint32_t len, uint32_t offset);

  int Update(const uint8_t *data, int n_bytes, uint32_t offset);

  void SegmentIsFull(); // Segment is full and all data is brushed to disk.
  
  int32_t GetCacheBlockId(uint32_t block_id);

 protected:
  // virtual int Compress() = 0;

  // virtual int Uncompress() = 0;

  virtual void InitSubclass() = 0;

  virtual int WriteContent(const uint8_t *data, int len, uint32_t offset,
                           disk_io::AsyncWriter *disk_io) = 0;

  virtual int GetReadFunParameter(ReadFunParameter &parameter, uint32_t len, 
                                  uint32_t off) = 0;

  virtual int ReadContent(uint8_t *value, uint32_t len, uint32_t offset) = 0;

  virtual int SubclassUpdate(const uint8_t *data, int len, uint32_t offset) = 0;

  LRUCache<uint32_t, std::vector<uint8_t>, ReadFunParameter *> *lru_cache_;

  int fd_;

  Compressor *compressor_;

  uint32_t per_block_size_;

  uint32_t size_;

  int item_length_;

  uint32_t seg_block_capacity_;

  uint32_t seg_id_;

  uint32_t header_size_;

  uint32_t last_bid_in_disk_;
};

}  // namespace tig_gamma
