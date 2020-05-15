include!("aux_wrapper_data.rs");
include!("gamma_rust_wrapper.rs");
use std::env;
use std::io::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::io::BufReader;

//convert i32 to [u8]
fn i32Tou8(v: i32, vec: &mut Vec<u8>) {
    unsafe {
        let i32Ptr: *const i32 = &v as *const i32;
        let u8Ptr: *const u8 = i32Ptr as *const u8;
        vec.push(*u8Ptr.offset(0));
        vec.push(*u8Ptr.offset(1));
        vec.push(*u8Ptr.offset(2));
        vec.push(*u8Ptr.offset(3));
    }
}

fn read(profile_file: &str, vector_file: &str, docs: &mut Vec<Doc>) {
  let file = File::open(profile_file).unwrap();
  let mut fin = BufReader::new(file);
  let mut count = 0;
  for line in fin.lines() {
    let line_str = line.unwrap();
    let a: Vec<&str> = line_str.split("\t").collect();
    let mut fields_tmp: Vec<Field> = Vec::new();
 
    let mut field_no = 0;
    for v in &a {
      match field_no {
        0 => { let v_int = v.to_string().parse::<i32>().unwrap();
               let mut vec_u8: Vec<u8> = Vec::new();
               i32Tou8(v_int, &mut vec_u8);
               let f0 = Field {
                 name: String::from("_id"),
                 value: vec_u8,
                 source: String::from(""),
                 data_type: DataType::INT,
               };
               fields_tmp.push(f0);
                                       }  
        1 => { let f1 = Field {
                 name: String::from("url"),
                 value: v.as_bytes().to_vec(),
                 source: String::from(""),
                 data_type: DataType::STRING,
               };
               fields_tmp.push(f1);
             }
        2 => { let v_int = v.to_string().parse::<i32>().unwrap();
               let mut vec_u8: Vec<u8> = Vec::new();
               i32Tou8(v_int, &mut vec_u8);

               let f2 = Field {
                 name: String::from("cid1"),
                 value: vec_u8,
                 source: String::from(""),
                 data_type: DataType::INT,
               };
               fields_tmp.push(f2);

              }
        3 => { 
               let v_int = v.to_string().parse::<i32>().unwrap();
               let mut vec_u8: Vec<u8> = Vec::new();
               i32Tou8(v_int, &mut vec_u8);

               let f3 = Field {
                 name: String::from("cid2"),
                 value: vec_u8,
                 source: String::from(""),
                 data_type: DataType::INT,
               };
               fields_tmp.push(f3);
             }
        _ => { let v_int = v.to_string().parse::<i32>().unwrap();
               let mut vec_u8: Vec<u8> = Vec::new();
               i32Tou8(v_int, &mut vec_u8);

               let f4 = Field {
                 name: String::from("cid3"),
                 value: vec_u8,
                 source: String::from(""),
                 data_type: DataType::INT,
               };
               fields_tmp.push(f4);

             }
      }
      field_no = field_no + 1;
    }
    let doc_item = Doc {
      fields: fields_tmp,
    };
    docs.push(doc_item);
    count = count + 1;
  }

  let mut f = File::open(vector_file).unwrap();
  let mut r = BufReader::new(&f);
  let mut vector_count = 0; 
  unsafe {
    loop {
      if vector_count >= count {
        break;
      } 
      let mut buf: [u8; 4] = Default::default();
      r.read(&mut buf).unwrap();
      let mut n: u32 = 0;
      n = std::mem::transmute::<[u8; 4], u32>(buf);
    
      let array = &mut[0u8; 4 * 128];
      r.read_exact(array);
      let b: Vec<u8> = array.iter().cloned().collect();

      let f5 = Field {
                 name: String::from("embedding_vector"),
                 value: b,
                 source: String::from(""),
                 data_type: DataType::VECTOR,
               };
      docs[vector_count].fields.push(f5);

      vector_count = vector_count + 1;
    }
  }
}


fn main() {
  let mut arguments = Vec::new();
  for argument in env::args() {
    arguments.push(argument);
  }
  if arguments.len() != 3 {
    println!("Usage: {} [profile file] [vector file]\n", arguments[0]);
    return; 
  }

  let config = Config {
    path: String::from("dump"),
    log_dir: String::from("test_log"),
    max_doc_size: 1000000,
  };

  let vector_info = VectorInfo {
    name: String::from("embedding_vector"),
    data_type: DataType::FLOAT,
    is_index: true,
    dimension: 128,
    model_id: String::from("vgg16"),
    store_type: String::from("Mmap"),
    store_param: String::from("{\"cache_size\": 256}"),
    has_source: false,
  };

  let field_info1 = FieldInfo {
    name: String::from("_id"),
    data_type: DataType::INT,
    is_index: false,
  };

  let field_info2 = FieldInfo {
    name: String::from("url"),
    data_type: DataType::STRING,
    is_index: false,
  };

  let field_info3 = FieldInfo {
    name: String::from("cid1"),
    data_type: DataType::INT,
    is_index: false,
  };

  let field_info4 = FieldInfo {
    name: String::from("cid2"),
    data_type: DataType::INT,
    is_index: false,
  };

  let field_info5 = FieldInfo {
    name: String::from("cid3"),
    data_type: DataType::INT,
    is_index: false,
  };

  let real_fields: &mut Vec<FieldInfo> = &mut Vec::new();
  real_fields.push(field_info1);
  real_fields.push(field_info2);
  real_fields.push(field_info3);
  real_fields.push(field_info4);
  real_fields.push(field_info5);

  let vector_infos : &mut Vec<VectorInfo> = &mut Vec::new();
  vector_infos.push(vector_info);
  
  let table = Table {
    name: String::from("testdb"),
    fields: &real_fields,
    vectors_info: &vector_infos,
    vectors_num: 1,
    indexing_size: 8192, 
    retrieval_type: String::from("IVFPQ"),
    retrieval_param: String::from("{\"nprobe\" : 10, \"metric_type\" : \"L2\", \"ncentroids\" : 256,\"nsubvector\" : 64}"),
  };

  let docs: &mut Vec<Doc> = &mut Vec::new();
  docs.reserve(10000);
  read(&arguments[1].to_string(), &arguments[2].to_string(), docs);
 
  unsafe {
    // init engine  
    let engine: *mut c_void = init(&config);
    println!("-------------Initialize gamma engine success!-------------\n");
    
    // create table
    let ret: i32 = create_table(engine, table); 
    if ret != 0 {
       println!("Create table failed!\n");
    }
    println!("-------------Create table success!-------------\n");
   
    let mut doc_count = 0; 
    for doc in docs {
      let ret = add_or_update_doc(engine, doc);
      if ret != 0 {
         println!("add_or_update_doc error! ret = {}\n", ret);
      }
      doc_count = doc_count + 1;
    }
    println!("-------------Add {} docs success!-------------\n", doc_count);

    let mut f = File::open(&arguments[2]).unwrap();
    let mut r = BufReader::new(&f);
    let mut vector_count = 0; 
    let count = 1;
    loop {
      if vector_count >= count {
        break;
      } 
      let mut buf: [u8; 4] = Default::default();
      r.read(&mut buf).unwrap();
      let mut n: u32 = 0;
      n = std::mem::transmute::<[u8; 4], u32>(buf);
     
      let array = &mut[0u8; 4 * 128];
      r.read_exact(array);
      let b: Vec<u8> = array.iter().cloned().collect();

      let mut vec_query: Vec<VectorQuery> = Vec::new();

      let single_query = VectorQuery {
        name: String::from("embedding_vector"),
        value: &b,
        min_score: -88888.0,
        max_score: 8888888.0,
        boost: 1.0,
        has_boost: 1,
      };
      vec_query.push(single_query);

      let mut wrap_fields: Vec<String> = Vec::new();
      wrap_fields.push(String::from("_id"));
      wrap_fields.push(String::from("cid1"));
      wrap_fields.push(String::from("cid2"));

      let wrap_range_filters: Vec<RangeFilter> = Vec::new();
      let wrap_term_filters: Vec<TermFilter> = Vec::new();     
 

      let req = Request {
        req_num: 1,
        topn: 10,
        direct_search_type: 1,
        vec_fields: &vec_query,
        fields: &wrap_fields,
        range_filters: &wrap_range_filters,
        term_filters: &wrap_term_filters,
        metric_type: DistanceMetricType::L2,
        online_log_level: String::from("INFO"),
        has_rank: 1,
        multi_vector_rank: 0,
        parallel_based_on_query: true,
        l2_sqrt: false, 
      }; 
   
      let re: &mut Vec<SearchResult> = &mut Vec::new(); 
      let mut res: Response = Response {
        results: re,
        online_log_message: String::from("log"), 

      }; 

      let ret = search(engine, req, &mut res); 


      if (ret != 0) {
        println!("+++++++++++++++++++search failed! ret = {}++++++++++++", ret);
      }
      println!("-------------Search interface success!-------------\n");
      vector_count = vector_count + 1;
    }
  }
}
