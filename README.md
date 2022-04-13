## Elasticsearch导入导出

### 项目简介

Elasticsearch导入导出的python脚本，有不加密版（normal_version）和加密版（rsa_encrypt_version），项目结构如下：

```
es-data-transfer
 ├── LICENSE
 ├── README.md
 ├── normal_version
 │   ├── config.ini.example
 │   ├── export_es_data.py
 │   ├── import_es_data.py
 │   └── json_file
 └── rsa_encrypt_version
     ├── export_es_data
     │   ├── config.ini.example
     │   ├── es_encrypt_file
     │   ├── es_json
     │   ├── es_zip
     │   ├── export_es_data.py
     │   └── rsa_public.pem
     ├── import_es_data
     │   ├── config.ini.example
     │   ├── es_decrypt_file
     │   ├── es_unzip
     │   ├── es_zip
     │   ├── import_es_data.py
     │   └── private_rsa_key.bin
     └── rsa_encryption.py
```

统一说明：

[1] config.ini.example用来配置ES的配置信息及同步的索引，修改后将其重命名为config.ini。

[2] ES依赖注意要和ES服务端的版本保持一致。

```
$ pip install elasticsearch==7.16.2 
```

### 不加密版

Step1：修改config.ini配置

Step2：执行脚本导入导出

执行 export_es_data.py 会读取 [SOURCE_ES] 里的 ES 配置，对指定索引进行导出，注意单次仅能导出10000条数据

执行 import_es_data.py 会读取 [TARGET_ES] 里的 ES 配置，json_file文件夹内的json文件进行导入，导入成功后会删除这些json文件。

### 加密版

Step1：修改config.ini配置

Step2：生成RSA公私钥

rsa_encryption.py是生成RSA公私钥的工具类，把密钥修改成自己的，然后执行该工具类生成rsa_public.pem公钥和private_rsa_key.bin私钥。将公钥放到export_es_data里用于加密，将私钥放到import_es_data里用于解密。再把import_es_data.py里的密钥换成与工具类一致的。

Step3：执行脚本导入导出

进入 export_es_data 目录，执行 export_es_data.py 导出数据并加密压缩

将 export_es_data/es_zip 里的压缩包拷贝到 import_es_data/es_zip里去

进入 import_es_data 目录，执行 import_es_data.py 解压解密并导入数据