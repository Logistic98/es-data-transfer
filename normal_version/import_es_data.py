# -*- coding: utf-8 -*-

import os
import logging
import time

from elasticsearch import Elasticsearch, helpers
from configparser import ConfigParser


# 生成日志文件
logging.basicConfig(filename='logging_es.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 查找解密后的json文件
def json_es(BASE_DIR):
    json_path = BASE_DIR + '/json_file/'
    filelist = []
    for file in os.listdir(json_path):
        if '.json' == file[-5:]:
            filelist.append(json_path + file)
    for i in filelist:
        head, sep, tail = i.partition('json_file/')
        indexname = tail
        head, sep, tail = indexname.partition('.json')
        index_name = head
        read_json(i, index_name)
        os.remove(i)


# 读json文件
def read_json(file_path, index_name):
    with open(file_path, 'r', encoding='utf-8') as file:
        json_str = file.read()
        # json_str中会存在一个null字符串表示空值，但是python里面没有null这个关键字，需要将null定义为变量名，赋值python里面的None
        null = None
        # 将字符串形式的列表数据转成列表数据
        json_list = eval(json_str)
        batch_data(json_list, index_name)


# 将构造好的列表写入ES数据库
def batch_data(json_list, index_name):
    """ 批量写入数据 """
    # 按照步长分批插入数据库,缓解插入数据库时的压力
    length = len(json_list)
    # 步长为1000,缓解批量写入的压力
    step = 1000
    for i in range(0, length, step):
        # 要写入的数据长度大于步长，那么久分批写入
        if i + step < length:
            actions = []
            for j in range(i, i + step):
                # 先把导入时添加的"_id"的值取出来
                new_id = json_list[j]['_id']
                del json_list[j]["_id"]  # 要删除导入时添加的"_id"
                action = {
                    "_index": str(index_name),
                    "_id": str(new_id),
                    "_source": json_list[j]
                }
                actions.append(action)
            helpers.bulk(Es, actions, request_timeout=120)
        # 要写入的数据小于步长，那么久一次性写入
        else:
            actions = []
            for j in range(i, length):
                # 先把导入时添加的"_id"的值取出来
                new_id = json_list[j]['_id']
                del json_list[j]["_id"]  # 要删除导入时添加的"_id"
                action = {
                    "_index": str(index_name),
                    "_id": str(new_id),
                    "_source": json_list[j]
                }
                actions.append(action)
            helpers.bulk(Es, actions, request_timeout=120)
    now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    insert_es_info = str(index_name) + "索引插入了" + str(length) + "条数据,时间是" + str(now_time)
    logging.info(insert_es_info)


def read_config():
    cfg = ConfigParser()
    cfg.read('./config.ini', encoding='utf-8')
    host = cfg.get('TARGET_ES', 'host')
    port = cfg.get('TARGET_ES', 'port')
    user = cfg.get('TARGET_ES', 'user')
    password = cfg.get('TARGET_ES', 'password')
    timeout = cfg.get('TARGET_ES', 'timeout')
    es_dict = {}
    es_dict['host'] = host
    es_dict['port'] = port
    es_dict['user'] = user
    es_dict['password'] = password
    es_dict['timeout'] = timeout
    return es_dict


if __name__ == '__main__':
    # 获取当前的目录地址
    BASE_DIR = os.getcwd()
    # 读取配置文件
    es_dict = read_config()
    # 构造连接
    Es = Elasticsearch(
        hosts=[str(es_dict['host']) + ":" + str(es_dict['port'])],
        http_auth=(str(es_dict['user']), str(es_dict['password'])),
        timeout=int(es_dict['timeout'])
    )
    # 将解密的json文件写入ES数据库
    json_es(BASE_DIR)
