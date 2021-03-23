# coding=utf-8
import json
import psutil
import shutil
import socket
import os.path
import inspect
import hashlib
import time
import datetime
import requests.adapters
from functools import reduce
import requests.sessions

from Config import BaseConfig
from Utils import RedisUtils
from common_logger.wrapper_hook_requests import log_normal_trace, log_error_trace


class CSVFileReader(object):
    """类 CSV 文件读取类，按行迭代返回分割后的文本内容"""

    def __init__(
            self, path: str, skip_first_line: bool, encoding: str = "utf-8", separator: str = "\t", max_split: int = -1
    ):
        """
        :param path: 文件路径
        :param skip_first_line: 是否跳过首行
        :param encoding: 文件编码
        :param separator: 分隔符
        :param max_split: 最大分割次数
        """

        self.path = path
        self.encoding = encoding
        self.separator = separator
        self.max_split = max_split
        self.skip_first_line = skip_first_line

    def __iter__(self):
        """迭代器"""

        with open(self.path, encoding=self.encoding) as fr:
            for index, line in enumerate(fr):
                if self.skip_first_line and index == 0:
                    continue
                if line.endswith("\n"):
                    line = line[:-1]
                if not line:
                    continue
                parts = tuple(line.split(self.separator, self.max_split))
                yield parts

    def __enter__(self):
        """上下文管理器"""

        return self.__iter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器"""

        pass


class RewriteSession(requests.sessions.Session):
    """重写Session对象，添加log"""

    def __init__(self, is_log=True):
        super(RewriteSession, self).__init__()
        self.is_log = is_log

    def request(self, method, url,
                params=None, data=None, headers=None, cookies=None, files=None,
                auth=None, timeout=None, allow_redirects=True, proxies=None,
                hooks=None, stream=None, verify=None, cert=None, json=None):
        start_time = time.time()
        response = super().request(method, url,
                                   params=params, data=data, headers=headers, cookies=cookies, files=files,
                                   auth=auth, timeout=timeout, allow_redirects=allow_redirects, proxies=proxies,
                                   hooks=hooks, stream=stream, verify=verify, cert=cert, json=json)
        if self.is_log:
            log_normal_trace(method, params, response, start_time, url)
        return response


def init_http_session(headers=None, params=None, retry=0, is_log=True):
    """初始化http session"""
    if is_log:
        http_session = RewriteSession()
    else:
        http_session = RewriteSession(is_log=False)

    if headers:
        http_session.headers.update(headers)

    if params:
        http_session.params.update(params)

    if retry:
        request_retry = requests.adapters.HTTPAdapter(max_retries=retry)
        http_session.mount("http://", request_retry)
        http_session.mount("https://", request_retry)

    return http_session


def init_redis_client():
    """初始化 redis 客户端，作为局部变量，复用连接池全局变量"""

    return RedisUtils.Redis(connection_pool=BaseConfig.redis_conn_pool)


def init_mysql_session(mode="r"):
    """初始化 mysql session，作为局部变量，复用工厂对象，相同线程多次初始化获得相同 session 对象"""

    factory = BaseConfig.mysql_session_factory_r if mode == "r" else BaseConfig.mysql_session_factory_w
    return factory()


def md5(s):
    """计算md5"""

    m = hashlib.md5()
    m.update(s.encode("utf-8"))
    return m.hexdigest()


def ts_to_str(ts):
    """"""

    class UTC(datetime.tzinfo):
        """UTC"""

        def __init__(self, offset=0):
            self._offset = offset

        def utcoffset(self, dt):
            return datetime.timedelta(hours=self._offset)

        def tzname(self, dt):
            return "UTC +%s" % self._offset

        def dst(self, dt):
            return datetime.timedelta(hours=self._offset)

    return datetime.datetime.fromtimestamp(ts, tz=UTC(8)).strftime("%Y-%m-%d %H:%M:%S")


class CacheContext(object):
    """缓存上下文管理"""

    def __init__(self, path: str):
        """
        初始化
        :param path: 缓存目录
        """

        self.path = path if path.startswith("/") else "{}/{}".format(os.getcwd(), path)

    def __enter__(self):
        """创建/清空目标目录，存在同名文件时先删除"""

        if os.path.exists(self.path) and not os.path.isdir(self.path):
            os.remove(self.path)
        if os.path.exists(self.path):
            shutil.rmtree(self.path)
        else:
            os.mkdir(self.path)

        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        """删除目标目录"""

        shutil.rmtree(self.path)


class TmpContext(object):
    """缓存文件管理"""

    def __init__(self, name, target=None, interval="days", expire=3, clean=True):
        """
        初始化文件管理对象
        :param name: 缓存数据名称
        :param target: 缓存日期，datetime 对象
        :param interval: 分割区间，支持周 / 天 / 小时，对应 weeks / days / hours
        :param expire: 缓存目录保留数量
        :param clean: 进入和退出时是否清理
        """

        if interval == "weeks":
            fmt = "%W"
        elif interval == "days":
            fmt = "%Y%m%d"
        elif interval == "hours":
            fmt = "%Y%m%d%H"
        else:
            raise ValueError("unknown param interval='{}'".format(interval))

        if target:
            self.target = datetime.datetime.strptime(target, fmt)
        else:
            self.target = datetime.datetime.today()
        self.clean = clean
        self.expire = expire
        self.interval = interval
        self.fmt = "{}/{}/{}_{}".format(BaseConfig.path_tmp, name, name, fmt)
        self.path = self.target.strftime(self.fmt)

    def __enter__(self):
        """进入上下文管理器"""

        # 进入时清理，避免残余文件干扰
        if self.clean and os.path.exists(self.path):
            shutil.rmtree(self.path)
            os.makedirs(self.path)
        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器"""

        if exc_type:
            raise

        fmt_split = self.fmt.split("/")
        head, tail = "/".join(fmt_split[0: -1]), fmt_split[-1]
        dt = self.target - datetime.timedelta(**{self.interval: self.expire})
        for item in os.listdir(head):
            if item < dt.strftime(tail):
                path = "{}/{}".format(head, item)
                try:
                    shutil.rmtree(path)
                except NotADirectoryError:
                    # 解决 mac 开发环境下自动产生 ./DS_Store 文件
                    os.remove(path)



# todo: 尝试 retrying 包
def retry(times, include=None, exclude=None):
    """
    异常重试装饰器
    :param times: 重试次数
    :param include: 可选，可以触发重试的异常类型，必须是异常类型或异常类型元组
    :param exclude: 可选，不可触发重试的异常类型，必须是异常类型或异常类型元组，和 include 同时存在时将被忽略
    """

    if include:
        exclude = None

    def decorator(func):
        """"""

        def wrapper(*args, **kwargs):
            """"""

            exception_counter = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if exception_counter < times:
                        if not (include or exclude):
                            exception_counter += 1
                        elif include and isinstance(e, include):
                            exception_counter += 1
                        elif exclude and (not isinstance(e, exclude)):
                            exception_counter += 1
                        else:
                            raise e
                    else:
                        raise e

        return wrapper

    return decorator


IN_SERVICE = 2
OUT_OF_SERVICE = 1
CHECK_SERVICE_ERROR = 0


def is_in_service():
    """
    检查脚本是否执行中，避免重复执行，无权限访问的进程无法检测
    :return: 以上常量之一
    """

    counter = 0
    # 脚本在命令中不一定为绝对路径
    # __file__ 为当前文件路径，使用调用栈获取调用者文件路径
    fn = inspect.stack()[1].filename.split(os.sep)[-1]
    for proc in psutil.process_iter():
        try:
            if proc.name().startswith("python") and reduce(lambda x, y: x or y.endswith(fn), proc.cmdline(), False):
                counter += 1
        # 访问无权限进程时触发异常
        except psutil.AccessDenied:
            pass
    counter = 2 if counter > 1 else counter
    return counter


def scan_port(ip, port, port_type="TCP"):
    """
    扫描端口占用情况
    :param ip: 待检测 ip，str 类型
    :param port: 待检测端口，str/int 类型
    :param port_type: 待检测协议，TCP/UDP，str 类型
    :return: bool，True 为占用
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM if port_type == "TCP" else socket.SOCK_DGRAM)
    in_use = not bool(sock.connect_ex((ip, int(port))))
    sock.close()
    return in_use


def get_location_by_city(city_name):
    """
    获取城市中心坐标
    :param city_name: 城市名称，str 类型
    :return: 坐标字典
    """

    with open("{}/Utils/cities.json".format(BaseConfig.path_project)) as fp:
        province_city = json.load(fp)

    location = dict(province="", city="")
    for province, city_list in province_city.items():
        for city in city_list:
            if city == city_name:
                location = dict(province=province, city=city)

    return location


if __name__ == '__main__':
    pass
