enum DataType { 
  INT = 0, 
  LONG = 1, 
  FLOAT = 2,
  DOUBLE = 3,
  STRING = 4, 
  VECTOR = 5,
}

struct Config {
  path: String,
  log_dir: String,
  max_doc_size: i32,
}

struct Field {
  name: String,
  value: Vec<u8>,
  source: String,
  data_type: DataType,
}

struct Doc {
  fields: Vec<Field>,
}

struct VectorInfo {
  name: String,
  data_type: DataType,
  is_index: bool,
  dimension: i32,
  model_id: String,
  store_type: String,
  store_param: String,
  has_source: bool,
}

struct FieldInfo {
  name: String,
  data_type: DataType,
  is_index: bool,
}

struct Table<'a, 'b> {
  name: String,
  fields: &'a Vec<FieldInfo>,
  vectors_info: &'b Vec<VectorInfo>,
  vectors_num: i32,
  indexing_size: i32,
  retrieval_type: String,
  retrieval_param: String,
}

struct TermFilter<'a> {
  field: String,
  value: &'a Vec<u8>,
  is_union: bool,
}

struct RangeFilter<'a> {
  field: String,
  lower_value: &'a Vec<u8>,
  upper_value: &'a Vec<u8>,
  include_lower: bool,
  include_upper: bool,
}

struct VectorQuery<'a> {
  name: String,
  value: &'a Vec<u8>,
  min_score: f64,
  max_score: f64,
  boost: f64,
  has_boost: i32,
}

enum DistanceMetricType { 
  InnerProduct = 0, 
  L2 = 1, 
}

struct Request<'a, 'b, 'c, 'd> {
  req_num: i32,                   // request number
  topn: i32,                       // top n similar results
  direct_search_type: i32,         // 1 : direct search; 0 : normal search
  vec_fields: &'a Vec<VectorQuery<'a> >,       // vec_field array
  fields: &'b Vec<String>,
  range_filters: &'c Vec<RangeFilter<'c> >,
  term_filters: &'d Vec<TermFilter<'d> >,
  metric_type: DistanceMetricType,
  online_log_level: String,        // DEBUG, INFO, WARN, ERROR
  has_rank: i32,                   // default 0, has not rank; 1, has rank
  multi_vector_rank: i32,
  parallel_based_on_query: bool,   // 
  l2_sqrt: bool,                   // default FALSE, don't do sqrt; TRUE, do sqrt
}

enum SearchResultCode { 
  SUCCESS = 0, 
  INDEX_NOT_TRAINED = 1, 
  SEARCH_ERROR = 2,
}

struct Attribute {
  name: String,
  value: Vec<u8>,
}

struct ResultItem {
  score: f64,
  attributes: Vec<Attribute>, 
  extra: String,
}

struct SearchResult {
  total: i32,
  result_code: SearchResultCode,
  msg: String,
  result_items: Vec<ResultItem>,
}

struct Response<'a> {
  results: &'a mut Vec<SearchResult>,
  online_log_message: String,
}

