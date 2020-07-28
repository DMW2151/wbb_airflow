#!/bin/bash

processF4File() { 
    local fn_tmp_file=$( mktemp ${filings_dir}/XXXXXXXXXX.json)
    echo $fn_tmp_file $filings_dir

    curl --silent $1 | \
        sed -n '/<ownershipDocument>/,/<\/ownershipDocument>/p' |\
        xq -r -j . |\
        jq --arg doc_no $2 '. + {doc_no: $doc_no}' >> $fn_tmp_file
}           

export -f processF4File