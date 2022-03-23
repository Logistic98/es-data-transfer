## Elasticsearch导入导出

Elasticsearch导入导出的python脚本，使用方法如下：

Step1：安装依赖

```
$ pip install elasticsearch==7.14.1   // 注意要和ES的版本保持一致
```

Step2：修改配置文件

config.ini（把ES连接信息换成自己的）

```ini
[TARGET_ES]
host = 192.168.1.1
port = 9200
user = elastic
password = elastic
timeout = 60

[SOURCE_ES]
host = 192.168.1.2
port = 9200
user = elastic
password = elastic
timeout = 60
index_list = test_index1, test_index2
```

注：多个索引之间用英文逗号分隔（逗号后面有没有空格都无所谓，读取配置时会进行处理）

Step3：执行脚本导入导出

执行 export_es_data.py 会读取 [SOURCE_ES] 里的 ES 配置，对指定索引进行导出，注意单次仅能导出10000条数据

执行 import_es_data.py 会读取 [TARGET_ES] 里的 ES 配置，json_file文件夹内的json文件进行导入，导入成功后会删除这些json文件。

 