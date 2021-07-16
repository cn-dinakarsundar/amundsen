# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import csv
import logging
import os
import shutil
from csv import DictWriter
from typing import (
    Any, Dict, FrozenSet,
)

from pyhocon import ConfigFactory, ConfigTree

from databuilder.job.base_job import Job
from databuilder.loader.base_loader import Loader
from databuilder.models.graph_serializable import GraphSerializable
from databuilder.serializers import neo4_serializer
from databuilder.utils.closer import Closer
import boto3
import botocore
import pandas as pd


s3 = boto3.resource('s3')

LOGGER = logging.getLogger(__name__)


class S3Neo4jCSVLoader(Loader):
    """
    Write node and relationship CSV file(s) that can be consumed by
    Neo4jCsvPublisher.
    It assumes that the record it consumes is instance of Neo4jCsvSerializable
    """
    # Config keys
    FORCE_CREATE_DIR = 'force_create_directory'
    SHOULD_DELETE_CREATED_DIR = 'delete_created_directories'
    # s3 key
    # s3 Bucket
    NODE_S3_BUCKET = 'node_s3_bucket'
    # Node s3 Prefix
    NODE_S3_PREFIX = 'node_s3_prefix'
    # Relations s3 Prefix
    RELATION_S3_PREFIX = 'relations_s3_prefix'

    _DEFAULT_CONFIG = ConfigFactory.from_dict({
        SHOULD_DELETE_CREATED_DIR: True,
        FORCE_CREATE_DIR: False
    })

    def __init__(self) -> None:
        self._node_file_mapping: Dict[Any, DictWriter] = {}
        self._relation_file_mapping: Dict[Any, DictWriter] = {}
        self._keys: Dict[FrozenSet[str], int] = {}
        self._closer = Closer()

    def init(self, conf: ConfigTree) -> None:

        """
        Initializing S3Neo4jCsvLoader by creating directory for node files
        and relationship files. Note that the directory defined in
        configuration should not exist.
        :param conf:
        :return:
        """
        conf = conf.with_fallback(S3Neo4jCSVLoader._DEFAULT_CONFIG)

        self._s3_bucket = conf.get_string(S3Neo4jCSVLoader.NODE_S3_BUCKET)
        self._node_s3_prefix= conf.get_string(S3Neo4jCSVLoader.NODE_S3_PREFIX)
        self._relation_s3_prefix = \
            conf.get_string(S3Neo4jCSVLoader.RELATION_S3_PREFIX)

    def _s3_obj_exists(self, node_dict, key, _node_dir, file_suffix, bucket_info) :
        try :
            s3.Object(bucket_info, _node_dir + file_suffix+'.csv').load()
            return True
        except botocore.exceptions.ClientError as e :
            if e.response['Error']['Code'] == "404" :
                return False

    def _get_s3_object(self ,node_dict, key,_s3_bucket_info, _node_s3_prefix, file_suffix) :
        if (self._s3_obj_exists(node_dict, key, _node_s3_prefix, file_suffix, _s3_bucket_info) == True) :
            df = pd.read_csv('s3://' + _s3_bucket_info + '/' + _node_s3_prefix + file_suffix+'.csv')
            df2 = pd.DataFrame.from_records(node_dict,index=[0])
            df = df.append(df2, ignore_index=True)
            df.to_csv('s3://' + _s3_bucket_info + '/' + _node_s3_prefix + file_suffix+'.csv', index=False)
        else :
            df = pd.DataFrame.from_records(node_dict, index=[0])
            df.to_csv('s3://' + _s3_bucket_info +'/'+ _node_s3_prefix + file_suffix+'.csv', index=False)

    def update_acls(self, s3_bucket_name,s3_prefix):
        my_bucket = s3.Bucket(s3_bucket_name)
        objects = my_bucket.objects.filter(Prefix=s3_prefix)
        for obj in objects :
            print(obj.Acl().put(ACL='public-read'))


    def load(self, csv_serializable: GraphSerializable) -> None:
        """
        Writes Neo4jCsvSerializable into CSV files.
        There are multiple CSV files that this method writes.
        This is because there're not only node and relationship, but also it
        can also have different nodes, and relationships.

        Common pattern for both nodes and relations:
         1. retrieve csv row (a dict where keys represent a header,
         values represent a row)
         2. using this dict to get a appropriate csv writer and write to it.
         3. repeat 1 and 2

        :param csv_serializable:
        :return:
        """

        node = csv_serializable.next_node()
        while node:
            node_dict = neo4_serializer.serialize_node(node)
            key = (node.label, self._make_key(node_dict))
            file_suffix = '{}_{}'.format(*key)
            self._get_s3_object(node_dict, key,self._s3_bucket, self._node_s3_prefix, file_suffix)
            node = csv_serializable.next_node()

        self.update_acls(self._s3_bucket,self._node_s3_prefix)

        relation = csv_serializable.next_relation()
        while relation:
            relation_dict = neo4_serializer.serialize_relationship(relation)
            key2 = (relation.start_label,
                    relation.end_label,
                    relation.type,
                    self._make_key(relation_dict))

            file_suffix = f'{key2[0]}_{key2[1]}_{key2[2]}'
            self._get_s3_object(relation_dict, key2,self._s3_bucket,self._relation_s3_prefix, file_suffix)
            relation = csv_serializable.next_relation()

    def get_scope(self) -> str:
        return "loader.filesystem_csv_neo4j"

    def _make_key(self, record_dict: Dict[str, Any]) -> int:
        """ Each unique set of record keys is assigned an increasing numeric key """
        return self._keys.setdefault(frozenset(record_dict.keys()), len(self._keys))
