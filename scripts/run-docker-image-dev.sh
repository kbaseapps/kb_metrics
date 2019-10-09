#!/usr/bin/bash

#!/bin/bash
root=$(git rev-parse --show-toplevel)
source_dir=lib
container_root=/kb/module
# if [ -z $env ]; then
# 	echo "The 'env' environment variable is required"
# 	exit 1
# fi
# use this below if you want to install and mount external components (bower packages) into the running container.
# docker run \
# 	--dns=8.8.8.8 \
# 	-e "CONFIG_ENV=${env}" \
# 	--network=kbase-dev \
# 	--name=narrative  \
# 	--mount type=bind,src=${root}/${source_dir},dst=${container_root}/${source_dir} \
# 	--rm kbase/narrative:dev


docker run -i -t \
  --network=kbase-dev \
  --name=kb_metrics  \
  --dns=8.8.8.8 \
  -e "KBASE_ENDPOINT=https://ci.kbase.us/services" \
  -e "AUTH_SERVICE_URL=https://ci.kbase.us/services/auth/api/legacy/KBase/Sessions/Login" \
  -e "AUTH_SERVICE_URL_ALLOW_INSECURE=true" \
  -p 5000:5000 \
  --mount type=bind,src=${root}/test_local/workdir,dst=${container_root}/work \
  --mount type=bind,src=${root}/${source_dir},dst=${container_root}/${source_dir} \
  --rm test/kb_metrics:dev 
