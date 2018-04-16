#!/bin/bash

path=`dirname $0`
cd ${path}

cp data-collection/* deploy_dir/
cd deploy_dir
zip -r deploy.zip *
aws s3 cp deploy.zip s3://smart-quant-data/code_deploy/
rm -rf deploy.zip
