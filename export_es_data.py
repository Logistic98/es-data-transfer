# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from datetime import timedelta
import datetime
import os
import json
import logging
from configparser import ConfigParser

# 生成日志文件
logging.basicConfig(filename='logging_es.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_config():
    cfg = ConfigParser()
    cfg.read('./config.ini', encoding='utf-8')
    host = cfg.get('SOURCE_ES', 'host')
    port = cfg.get('SOURCE_ES', 'port')
    user = cfg.get('SOURCE_ES', 'user')
    password = cfg.get('SOURCE_ES', 'password')
    timeout = cfg.get('SOURCE_ES', 'timeout')
    index_list = cfg.get('SOURCE_ES', 'index_list')
    es_dict = {}
    es_dict['host'] = host
    es_dict['port'] = port
    es_dict['user'] = user
    es_dict['password'] = password
    es_dict['timeout'] = timeout
    es_dict['index_list'] = index_list
    return es_dict


def write_list_to_json(list, json_file_name, json_file_save_path):
    """
    将list写入到json文件
    :param list:
    :param json_file_name: 写入的json文件名字
    :param json_file_save_path: json文件存储路径
    :return:
    """
    if not os.path.exists(json_file_save_path):
        os.makedirs(json_file_save_path)
    os.chdir(json_file_save_path)
    with open(json_file_name, 'w', encoding='utf-8') as f:
        json.dump(list, f, ensure_ascii=False)


def es_json(es_dict, start_time, end_time):
    str_separate = "==============================================================="
    try:
        BASE_DIR = os.getcwd()
        Es = Elasticsearch(
            hosts=[str(es_dict['host']) + ":" + str(es_dict['port'])],
            http_auth=(str(es_dict['user']), str(es_dict['password'])),
            timeout=int(es_dict['timeout'])
        )

    except Exception as e:
        logging.error(e)

    index_list = ''.join(es_dict['index_list'].split()).split(",")
    for i in index_list:
        print(f"保存索引{i}的数据\r")
        print_info1 = "保存索引" + i + "的数据"
        logging.info(print_info1)
        query = {
            "query": {
                "range": {
                    "@timestamp": {
                        # 大于上一次读取结束时间，小于等于本次读取开始时间
                        "gt": start_time,
                        "lte": end_time
                    }
                }
            },
            "size": 10000
        }
        try:
            data = Es.search(index=i, body=query)
            source_list = []
            for hit in data['hits']['hits']:
                source_data = hit['_source']
                source_data['_id'] = hit['_id']
                source_list.append(source_data)
            print(f"保存的时间为{start_time}到{end_time}\r")
            print_info2 = "保存的时间为" + start_time + "到" + end_time + ""
            logging.info(print_info2)
            file_path = BASE_DIR + "/json_file"
            file_name = str(i) + ".json"
            if len(source_list) != 0:
                write_list_to_json(source_list, file_name, file_path)
            else:
                print('无更新')
                logging.info(str(i) + '无更新')
            print(str_separate)
            logging.info(str_separate)
        except Exception as e:
            print(e)
            logging.info("es数据库到json文件的读写error" % e)
            logging.info(str_separate)


if __name__ == '__main__':
    start_date_time = datetime.datetime.now() + timedelta(days=-1)
    end_date_time = datetime.datetime.now()
    start_time = start_date_time.strftime("%Y-%m-%dT%H:00:00.000Z")
    end_time = end_date_time.strftime("%Y-%m-%dT%H:00:00.000Z")
    # 读取配置信息
    es_dict = read_config()
    # 获取当前的目录地址
    BASE_DIR = os.getcwd()
    # 读取es数据库中的数据，写成json文件
    es_json(es_dict, start_time, end_time)

