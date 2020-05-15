extern crate libc;
extern crate flatbuffers;
mod config_generated;

mod types_generated;
mod table_generated;
mod doc_generated;
mod request_generated;
mod response_generated;

use libc::{c_void, c_char};

#[link(name = "gamma")]
extern {
    fn Init(config_str: *const c_char, len: i32) -> *mut c_void;

    fn Close(engine: *mut c_void) -> i32;

    fn CreateTable(engine: *mut c_void, table_str: *const c_char, len: i32) -> i32;

    fn AddOrUpdateDoc(engine: *mut c_void, doc: *const c_char, len: i32) -> i32;

    fn UpdateDoc(engine: *mut c_void, doc: *const c_char, len: i32) -> i32;

    fn DeleteDoc(engine: *mut c_void, docid: i64) -> i32;    

    fn GetEngineStatus(engine: *mut c_void, status: *mut *mut c_char, len: *mut i32) -> i32;

    fn GetDocByID(engine: *mut c_void, docid: i64, doc: *mut *mut c_char, len: *mut i32) -> i32;

    fn Search(engine: *mut c_void, request: *const c_char, req_len: i32, response: *mut *mut c_char, res_len: *mut i32) -> i32;

    fn DelDocByQuery(engine: *mut c_void, request: *const c_char, len: i32) -> i32;
}



unsafe fn init(config: &Config) -> *mut c_void {
  let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
  let path_name = builder.create_string(&config.path);
  let log_path = builder.create_string(&config.log_dir);
  let config = config_generated::gamma_api::Config::create(&mut builder, &config_generated::gamma_api::ConfigArgs{
    path: Some(path_name),
    log_dir: Some(log_path),
    max_doc_size: config.max_doc_size,
  });
  builder.finish(config, None);
  let buf = builder.finished_data();
  let engine = Init(buf.as_ptr() as *const c_char, 0);
  engine
}

unsafe fn create_table(engine: *mut c_void, table: Table) -> i32 {
  let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
  let table_name = builder.create_string(&table.name); 
 
  let mut f_fields: Vec<flatbuffers::WIPOffset<table_generated::gamma_api::FieldInfo>> = Vec::new(); 
  for item in table.fields {
    let f_name = builder.create_string(&item.name);
    let mut f_type = types_generated::DataType::INT;
    match item.data_type {
      DataType::INT => { } 
      DataType::LONG => { f_type = types_generated::DataType::LONG; }
      DataType::FLOAT => { f_type = types_generated::DataType::FLOAT; }
      DataType::DOUBLE => { f_type = types_generated::DataType::DOUBLE; }
      DataType::STRING => { f_type = types_generated::DataType::STRING; }
      DataType::VECTOR => { f_type = types_generated::DataType::VECTOR; }
    }
    let f = table_generated::gamma_api::FieldInfo::create(&mut builder, &table_generated::gamma_api::FieldInfoArgs {
      name: Some(f_name), 
      data_type: f_type, 
      is_index: item.is_index,
    });
    f_fields.push(f);
  } 

  let mut v_vectors: Vec<flatbuffers::WIPOffset<table_generated::gamma_api::VectorInfo>> = Vec::new(); 
  for info in table.vectors_info {
    let v_name = builder.create_string(&info.name);
    let mut v_type = types_generated::DataType::INT;
    match info.data_type {
      DataType::INT => { } 
      DataType::LONG => { v_type = types_generated::DataType::LONG; }
      DataType::FLOAT => { v_type = types_generated::DataType::FLOAT; }
      DataType::DOUBLE => { v_type = types_generated::DataType::DOUBLE; }
      DataType::STRING => { v_type = types_generated::DataType::STRING; }
      DataType::VECTOR => { v_type = types_generated::DataType::VECTOR; }
    }
    let v_model_id = builder.create_string(&info.model_id);
    let v_store_type = builder.create_string(&info.store_type);
    let v_store_param = builder.create_string(&info.store_param);
    
    let v = table_generated::gamma_api::VectorInfo::create(&mut builder, &table_generated::gamma_api::VectorInfoArgs {
      name: Some(v_name), 
      data_type: v_type, 
      is_index: info.is_index,
      dimension: info.dimension,
      model_id: Some(v_model_id),
      store_type: Some(v_store_type),
      store_param: Some(v_store_param),
      has_source: info.has_source,
    });
    v_vectors.push(v);
  } 

  let fields_vec = builder.create_vector(&f_fields[..]);
  let vectors_vec = builder.create_vector(&v_vectors[..]);
  let retrieval_type_name = builder.create_string(&table.retrieval_type);
  let retrieval_param_name = builder.create_string(&table.retrieval_param);
  let table_struct = table_generated::gamma_api::Table::create(&mut builder, &table_generated::gamma_api::TableArgs {
    name: Some(table_name),
    fields: Some(fields_vec),
    vectors_info: Some(vectors_vec),
    vectors_num: table.vectors_num,
    indexing_size: table.indexing_size,
    retrieval_type:  Some(retrieval_type_name),
    retrieval_param:  Some(retrieval_param_name),	
  });
  builder.finish(table_struct, None);
  let buf = builder.finished_data();
  let ret = CreateTable(engine, buf.as_ptr() as *const c_char, 0);
  ret
}

unsafe fn add_or_update_doc(engine: *mut c_void, doc: &Doc) -> i32 {
  let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
  let mut doc_fields: Vec<flatbuffers::WIPOffset<doc_generated::gamma_api::Field>> = Vec::new(); 
  for doc_f in &doc.fields {
    let field_name = builder.create_string(&doc_f.name);
    let field_value = builder.create_vector(&doc_f.value[..]);
    let field_source = builder.create_string(&doc_f.source);
    let mut field_type = types_generated::DataType::INT;
    match doc_f.data_type {
      DataType::INT => { } 
      DataType::LONG => { field_type = types_generated::DataType::LONG; }
      DataType::FLOAT => { field_type = types_generated::DataType::FLOAT; }
      DataType::DOUBLE => { field_type = types_generated::DataType::DOUBLE; }
      DataType::STRING => { field_type = types_generated::DataType::STRING; }
      DataType::VECTOR => { field_type = types_generated::DataType::VECTOR; }
    }
    
    let f = doc_generated::gamma_api::Field::create(&mut builder, &doc_generated::gamma_api::FieldArgs {
      name: Some(field_name), 
      value: Some(field_value),
      source: Some(field_source),
      data_type: field_type, 
    });
    doc_fields.push(f);
  } 

  let fields_vec = builder.create_vector(&doc_fields[..]);

  let doc_struct = doc_generated::gamma_api::Doc::create(&mut builder, &doc_generated::gamma_api::DocArgs {
    fields: Some(fields_vec),
  });
  builder.finish(doc_struct, None);
  let buf = builder.finished_data();
  let ret = AddOrUpdateDoc(engine, buf.as_ptr() as *const c_char, 0);
  ret 
} 

unsafe fn search(engine: *mut c_void, request: Request, response: &mut Response) -> i32 {
  let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);
  let mut queries: Vec<flatbuffers::WIPOffset<request_generated::gamma_api::VectorQuery>> = Vec::new(); 
  for query in request.vec_fields {
    let query_name = builder.create_string(&query.name);
    let query_value = builder.create_vector(&query.value[..]);
    let q = request_generated::gamma_api::VectorQuery::create(&mut builder, &request_generated::gamma_api::VectorQueryArgs {
      name: Some(query_name), 
      value: Some(query_value),
      min_score: query.min_score, 
      max_score: query.max_score,
      boost: query.boost,
      has_boost: query.has_boost, 
    });
    queries.push(q);
  } 
  let queries_vec = builder.create_vector(&queries[..]);


  let mut q_fields: Vec<flatbuffers::WIPOffset<&str>> = Vec::new(); 

  for fi in request.fields {
    let fi_seq = builder.create_string(fi); 
    q_fields.push(fi_seq); 
  }

  let wrap_fields = builder.create_vector(&q_fields[..]);

  let mut range_filters: Vec<flatbuffers::WIPOffset<request_generated::gamma_api::RangeFilter>> = Vec::new(); 

  for r in request.range_filters {
    let r_field = builder.create_string(&r.field);
    let r_lower_value = builder.create_vector(&r.lower_value[..]);
    let r_upper_value = builder.create_vector(&r.upper_value[..]);
    let range_filter = request_generated::gamma_api::RangeFilter::create(&mut builder, &request_generated::gamma_api::RangeFilterArgs {
      field: Some(r_field),
      lower_value: Some(r_lower_value),
      upper_value: Some(r_upper_value),
      include_lower: r.include_lower,
      include_upper: r.include_upper,
    });
    
    range_filters.push(range_filter); 
  }
  let range_filters_seq = builder.create_vector(&range_filters[..]);

  let mut term_filters: Vec<flatbuffers::WIPOffset<request_generated::gamma_api::TermFilter>> = Vec::new(); 

  for t in request.term_filters {
    let t_field = builder.create_string(&t.field);
    let t_value = builder.create_vector(&t.value[..]);
    let term_filter = request_generated::gamma_api::TermFilter::create(&mut builder, &request_generated::gamma_api::TermFilterArgs {
      field: Some(t_field),
      value: Some(t_value),
      is_union: t.is_union,
    });
    
    term_filters.push(term_filter); 
  }
  let term_filters_seq = builder.create_vector(&term_filters[..]);

  let mut metric_type_seq = request_generated::gamma_api::DistanceMetricType::InnerProduct;
  match request.metric_type {
      DistanceMetricType::InnerProduct => { } 
      DistanceMetricType::L2 => { metric_type_seq = request_generated::gamma_api::DistanceMetricType::L2; }
  }

  let online_log = builder.create_string(&request.online_log_level);

  let request_struct = request_generated::gamma_api::Request::create(&mut builder, &request_generated::gamma_api::RequestArgs {
    req_num: request.req_num,
    topn: request.topn,
    direct_search_type: request.direct_search_type,
    vec_fields: Some(queries_vec),
    fields: Some(wrap_fields),
    range_filters: Some(range_filters_seq),
    term_filters: Some(term_filters_seq),
    metric_type: metric_type_seq,
    online_log_level: Some(online_log),
    has_rank: request.has_rank,
    multi_vector_rank: request.multi_vector_rank,
    parallel_based_on_query: request.parallel_based_on_query,
    l2_sqrt: request.l2_sqrt
  });
  builder.finish(request_struct, None);
  let buf = builder.finished_data();
  let len = 255;
  let mut tmp = Vec::<u8>::with_capacity(len);
  let mut res = tmp.as_mut_ptr() as *mut c_char; 

  let res_ptr = &mut res as *mut *mut c_char;
  let mut res_len: i32 = 0;
  let res_len_ptr = &mut res_len as *mut i32;
  let ret = Search(engine, buf.as_ptr() as *const c_char, 0, res_ptr, res_len_ptr);
   
  let res_buf: &[u8] = std::slice::from_raw_parts(*res_ptr as *const u8, res_len as usize); 

  let res_struct = response_generated::gamma_api::get_root_as_response(res_buf);
  
  let results_set = res_struct.results().unwrap();
  let results_size = results_set.len();

  let search_results: &mut Vec<SearchResult> = &mut response.results; 
  let mut i: usize = 0;
  let mut j: usize = 0;

  while i < results_size {
    let search_result = results_set.get(i);
    let r_total = search_result.total();
    
    let mut code = SearchResultCode::SUCCESS;
    match search_result.result_code() {
      response_generated::gamma_api::SearchResultCode::SUCCESS => { } 
      response_generated::gamma_api::SearchResultCode::INDEX_NOT_TRAINED => { code = SearchResultCode::INDEX_NOT_TRAINED; }
      response_generated::gamma_api::SearchResultCode::SEARCH_ERROR => { code = SearchResultCode::SEARCH_ERROR; }
    }
    let r_msg = search_result.msg().unwrap().to_string();
    let items = search_result.result_items().unwrap();
    let items_size = items.len();
    let mut items_vec: Vec<ResultItem> = Vec::new();
    while j < items_size {
      let each_item = items.get(j);
      let r_score = each_item.score();
      
      let r_extra = each_item.extra().unwrap().to_string();
      let fields = each_item.attributes().unwrap();
      let fields_size = fields.len();
      let mut fields_vec: Vec<Attribute> = Vec::new();
      let mut k: usize = 0;
      while k < fields_size {
        let each_field = fields.get(k);
        let f_name = each_field.name().unwrap().to_string();
        let f_value = each_field.value().unwrap();
        let attr = Attribute {
          name: f_name,
          value: f_value.to_vec(),
        };
        fields_vec.push(attr);
        k = k + 1;
      }
      let r_item = ResultItem {
        score: r_score,
        attributes: fields_vec, 
        extra: r_extra,
      };
      items_vec.push(r_item);
      j = j + 1;
    }
    let s_search_result = SearchResult {
      total:  r_total,
      result_code:  code,
      msg:  r_msg,
      result_items: items_vec,
    };
    search_results.push(s_search_result); 
    i = i + 1; 
  }  
  response.online_log_message =  res_struct.online_log_message().unwrap().to_string();
  ret 
} 
