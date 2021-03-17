import os
import sys
from datetime import timedelta
from Utils import InitUtils

# 目录
path_project, path_log, path_tmp = InitUtils.init_project_directory()

# Python 版本
is_py2 = sys.version.startswith("2")
is_py3 = not is_py2

# 周边服务
prod_conf_dict = dict(
    kafka_topic="",
    kafka_server=[],
    redis_server="127.0.0.1:3000/0",
    mysql_r_server="",
    mysql_w_server="",
)

pre_conf_dict = dict(
    kafka_topic="",
    kafka_server=[],
    redis_server="",
    mysql_r_server="",
    mysql_w_server="",
)

test_conf_dict = dict(
    kafka_topic="",
    kafka_server=[],
    redis_server="127.0.0.1:3000/0",
    mysql_r_server="",
    mysql_w_server="",
)

cloud_env_type = os.getenv("")
ENV_TYPE = 1

if cloud_env_type == "online":
    conf_dict = prod_conf_dict
    ENV_TYPE = 2
elif cloud_env_type == "pre":
    conf_dict = pre_conf_dict
else:
    conf_dict = test_conf_dict

kafka_topic = conf_dict["kafka_topic"]
kafka_server = conf_dict["kafka_server"]
redis_server = conf_dict["redis_server"]
mysql_r_server = conf_dict["mysql_r_server"]
mysql_w_server = conf_dict["mysql_w_server"]

# 全局变量
redis_conn_pool = InitUtils.init_redis_connection_pool(redis_server)
mysql_session_factory_r = InitUtils.init_mysql_session_factory(f"mysql+pymysql://{mysql_r_server}/threat_intel")
mysql_session_factory_w = InitUtils.init_mysql_session_factory(f"mysql+pymysql://{mysql_w_server}/threat_intel")



