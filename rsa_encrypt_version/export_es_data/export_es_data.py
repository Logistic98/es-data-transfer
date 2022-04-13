# -*- coding: utf-8 -*-
import zipfile

from Crypto.Cipher import PKCS1_OAEP, AES
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes
from elasticsearch import Elasticsearch
from datetime import timedelta
import datetime
import os
import json
import logging
from configparser import ConfigParser

# 生成日志文件
logging.basicConfig(filename='../logging_es.log', level=logging.INFO,
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


def encrypt_json(BASE_DIR):
    '''
    将json文件加密
    :return:
    '''
    public_file_path = BASE_DIR + '/es_zip'
    private_file_path = BASE_DIR + '/es_encrypt_file'
    rsa_public_path = BASE_DIR + '/rsa_public.pem'

    # 文件夹所有文件加密
    for next_dir in os.listdir(public_file_path):
        # print(next_dir)
        json_name = public_file_path + "/" + next_dir
        json_name_new = private_file_path + "/" + next_dir
        # 二进制只读打开文件，读取文件数据
        with open(json_name, 'rb') as f:
            data = f.read()
        file_name_new = json_name_new + '.rsa'
        with open(file_name_new, 'wb') as out_file:
            # 收件人秘钥 - 公钥
            recipient_key = RSA.import_key(open(rsa_public_path).read())
            # 一个 16 字节的会话密钥
            session_key = get_random_bytes(16)
            # Encrypt the session key with the public RSA key
            cipher_rsa = PKCS1_OAEP.new(recipient_key)
            out_file.write(cipher_rsa.encrypt(session_key))
            # Encrypt the data with the AES session key
            cipher_aes = AES.new(session_key, AES.MODE_EAX)
            cipher_text, tag = cipher_aes.encrypt_and_digest(data)
            out_file.write(cipher_aes.nonce)
            out_file.write(tag)
            out_file.write(cipher_text)
            os.remove(json_name)  # 加密完之后删除该文件


def zip_dir(dir_path, zip_path):
    """
    压缩加密文件夹中的所有文件，以同步开始时间-结束时间命名，然后压缩到上传同步目录
    :param dir_path: 目标文件夹路径
    :param zip_path: 压缩后的文件夹路径
    :return:
    """
    try:
        zip = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)
        for root, dirnames, filenames in os.walk(dir_path):
            file_path = root.replace(dir_path, '')  # 去掉根路径，只对目标文件夹下的文件及文件夹进行压缩
            # 循环出一个个文件名
            for filename in filenames:
                zip.write(os.path.join(root, filename), os.path.join(file_path, filename))
                os.remove(dir_path + '/' + filename)  # 压缩到zip文件之后删除该文件
        zip.close()
    except Exception as e:
        print(e)
        logging.error(e)


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
            "range": {
                "@timestamp": {
                    # 大于上一次读取结束时间，小于等于本次读取开始时间
                    "gt": start_time,
                    "lte": end_time
                }
            }
        }
        try:
            data = Es.search(index=i, query=query, size=10000)
            source_list = []
            for hit in data['hits']['hits']:
                source_data = hit['_source']
                source_data['_id'] = hit['_id']
                source_list.append(source_data)
            print(f"保存的时间为{start_time}到{end_time}\r")
            print_info2 = "保存的时间为" + start_time + "到" + end_time + ""
            logging.info(print_info2)
            file_path = BASE_DIR + "/es_zip"
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


def zip_es_encrypt_file(BASE_DIR, file_name_zip):
    dir_path_name = BASE_DIR + '/es_encrypt_file'
    zip_filename = str(file_name_zip) + '.zip'
    new_zip_path_file = BASE_DIR + '/es_zip' + '/' + str(zip_filename)
    zip_dir(dir_path_name, new_zip_path_file)


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
    # 加密json文件
    encrypt_json(BASE_DIR)
    # 压缩加密文件夹中的所有文件，以日期命名
    start_date = start_date_time.strftime("%Y-%m-%d-%H")
    end_date = end_date_time.strftime("%Y-%m-%d-%H")
    file_name_zip = start_date + "--" + end_date
    zip_es_encrypt_file(BASE_DIR, file_name_zip)

