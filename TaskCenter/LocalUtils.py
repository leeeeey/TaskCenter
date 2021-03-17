import datetime


class Interval(object):
    """时间区间对象，左闭右开区间"""

    def __init__(self, ts_start, ts_end):
        """
        初始化
        :param ts_start: 开始时间，单位为秒，int 类型
        :param ts_end: 结束时间，单位为秒，int 类型
        """

        self.ts_start, self.ts_end = ts_start, ts_end

    def to_tuple(self, time_type, time_format=None, is_closed=False):
        """
        以指定格式输出时间区间元组
        :param time_type: 时间类型，str 类型，支持 str/datetime/int
        :param time_format: time_type=str 时，指定时间的格式化字符串，str 类型
        :param is_closed: 是否输出闭区间，bool 类型
        :return: 时间区间 (start, end)
        """

        if is_closed:
            interval = (self.ts_start, self.ts_end - 1)
        else:
            interval = (self.ts_start, self.ts_end)
        if time_type != "int":
            interval = map(datetime.datetime.fromtimestamp, interval)
        if time_type == "str":
            interval = map(lambda x: x.strftime(time_format), interval)
        return tuple(interval)