import os
import redis
import sqlalchemy.orm
# todo import common_logger
from Utils.LogUtils import common_logger


def init_project_directory():
    """
    初始化日志、缓存等工程目录
    :return: (path_project, path_log, path_tmp)，均为绝对路径
    """

    path_project = os.sep.join(__file__.split(os.sep)[:-2])  # 计算方式与该函数在工程中的相对位置有关
    path_log = "{}{}Log".format(path_project, os.sep)
    if not os.path.exists(path_log):
        os.mkdir(path_log)
    path_tmp = "{}{}Tmp".format(path_project, os.sep)
    if not os.path.exists(path_tmp):
        os.mkdir(path_tmp)

    return path_project, path_log, path_tmp


def init_redis_connection_pool(redis_server):
    """
    初始化 redis 连接池，作为全局变量
    :param redis_server: 服务端配置
    """

    return redis.ConnectionPool.from_url(f"redis://{redis_server}", retry_on_timeout=True)


def init_mysql_session_factory(uri):
    """初始化 mysql session 工厂"""

    # mysql，空闲链接或执行sql 超过 120s，连接将被中断   pool_pre_ping检查并保持连接的活性
    engine = sqlalchemy.create_engine(uri, echo=True, pool_recycle=115, pool_pre_ping=True)

    # scoped_session 使用本地线程
    return sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(bind=engine))


if __name__ == '__main__':
    pass
