#!/bin/bash
#Script for uploading data files to HDFS 
if [ $# != 1 ]; then
    echo "Missing filename."
    exit
fi

FILEPATH=$1
FILE=$(echo $FILEPATH | grep -Eo "[^/]+$")

CONTAINER=$(docker container ls | grep namenode | sed 's/ .*//')
echo "Uploading $FILE to $CONTAINER hdfs:/$FILE"
docker cp $FILEPATH $CONTAINER:/$FILE
docker exec -it $CONTAINER hadoop fs -put /$FILE /

