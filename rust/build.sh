#!/bin/bash

CUR_PATH=`pwd`

echo $CUR_PATH

# Generating the flatbuffers for gamma engineinterfaces
flatc -r -o $CUR_PATH/src $CUR_PATH/../idl/fbs/*.fbs

# export the environment variable for gamma engine library in OSX(also can be used in linux by LD_LIBRARY_PATH)
export DYLD_LIBRARY_PATH=$CUR_PATH/../release 

#compile
cargo build

# profile_10k.txt:fields(not include vector) in documents, can be found in the tests directory of gamma engine 
# siftsmall_base.fvecs: please download the ANN_SIFT10K dataset from ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz, and put the file in current directory
cargo run $CUR_PATH/../tests/profile_10k.txt  $CUR_PATH/siftsmall_base.fvecs
