#!/bin/bash

TAG=redshifttools
PKG=redshiftTools
DATE=$(date +%Y-%m-%d)
VERSION=$(grep Version: DESCRIPTION | awk '{print $2}')

export PATH=~/.local/bin:$PATH

docker images | grep ${TAG} | awk '{print $3}' | xargs docker rmi -f || true

docker build -f tests.dockerfile -t ${TAG} .
docker run -f ${TAG}

clean_branch=$(echo $GIT_BRANCH | sed 's.origin/..g')

docker run ${TAG} /root/.local/bin/aws s3 cp ${PKG}_${VERSION}.tar.gz s3://zapier-data-packages/${PKG}/${PKG}_${VERSION}_${clean_branch}_latest.tar.gz
docker run ${TAG} /root/.local/bin/aws s3 cp ${PKG}_${VERSION}.tar.gz s3://zapier-data-packages/${PKG}/${PKG}_${VERSION}_${clean_branch}_${DATE}.tar.gz
