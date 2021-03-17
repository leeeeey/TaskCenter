import json
import datetime
from sqlalchemy import Integer, Column, String, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import timedelta
from Table import TaskBatch

Base = declarative_base()


class TaskInfo(Base):
    __tablename__ = 'task_info'

    id = Column(Integer, primary_key=True)
    task_num = Column(String(255))
    task_name = Column(String(255))
    task_type = Column(Integer)
    online = Column(Integer)
    dependence = Column(Text)
    script = Column(String(255))
    script_args = Column(String(255))
    exec_unit = Column(String(255))
    exec_unit_param = Column(Integer)
    delay = Column(Integer)
    start_expire = Column(Integer)
    retry_max_times = Column(Integer)
    run_expire = Column(Integer)
    create_time = Column(String(255))
    update_time = Column(String(255))

    def to_dict(self):
        """"""

        return dict(
            id=self.id,
            task_num=self.task_num,
            task_name=self.task_name,
            task_type=self.task_type,
            online=self.online,
            dependence=self.dependence,
            script=self.script,
            script_args=self.script_args,
            exec_unit=self.exec_unit,
            exec_unit_param=self.exec_unit_param,
            delay=self.delay,
            start_expire=self.start_expire,
            retry_max_times=self.retry_max_times,
            run_expire=self.run_expire,
            create_time=self.create_time,
            update_time=self.update_time,
        )

    def create_new_task(self, start_dt, batch_num=1):
        """根据任务创建时间和任务批次，创建 TaskBatch 对象"""

        end_dt = self.get_next_end_dt(start_dt)
        tag_name = self._get_tag_name(self.task_name, start_dt, self.exec_unit)
        kwargs = dict(
            task_name=self.task_name,
            task_tag_name=tag_name,
            task_batch_name=f"{tag_name}_{batch_num}",
            exec_status=0,
            dependence=json.dumps(self._get_depend_tag(start_dt)),
            start_time=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            plan_time=self._get_plan_dt(end_dt).strftime("%Y-%m-%d %H:%M:%S"),
            plan_expire_time=self._get_plan_expire_dt(end_dt).strftime("%Y-%m-%d %H:%M:%S"),
            exec_time="0000-00-00 00:00:00",
            exit_time="0000-00-00 00:00:00",
            duration=0,
            retry=0,
        )

        return TaskBatch.TaskBatch(**kwargs)

    def get_next_start_dt(self, start_dt):
        """输入批次执行时间区间左边界，计算下次任务执行时间区间的左边界"""

        exec_unit_map = dict(minute="minutes", hour="hours", day="days")
        return start_dt + datetime.timedelta(**{exec_unit_map[self.exec_unit]: self.exec_unit_param})

    def get_next_end_dt(self, start_dt):
        """输入批次执行时间区间左边界，计算下次任务执行时间区间的左边界"""

        exec_unit_map = dict(minute="minutes", hour="hours", day="days")
        return start_dt + datetime.timedelta(**{exec_unit_map[self.exec_unit]: 1})

    def get_init_start_dt(self, create_dt):
        """给定时间，计算最近可完整执行的时间区间，返回左边界，用于任务首次创建批次"""

        exec_unit = self.exec_unit
        if exec_unit == "minute":
            start_dt = datetime.datetime.fromtimestamp(
                (int(create_dt.timestamp()) // 60 - 1) * 60)
        elif exec_unit == "hour":
            start_dt = datetime.datetime.fromtimestamp(
                (int(create_dt.timestamp()) // 3600 - 1) * 3600)
        else:
            # 考虑时区对天级任务的影响
            start_dt = datetime.datetime.fromtimestamp(
                (int(create_dt.timestamp() + 28800) // 86400 - 1) * 86400 - 28800)

        return start_dt

    @staticmethod
    def _get_tag_name(task_name, start_dt, exec_unit):
        """计算 Tag 名称，由于需要计算依赖批次的 Tag 名称，因此需要数据 task_name / exec_unit 等参数"""

        fmt_len_map = dict(day=8, hour=10, minute=12)
        length = fmt_len_map[exec_unit]
        return f"{task_name}_{start_dt.strftime('%Y%m%d%H%M%S')[:length]}"

    def _get_depend_tag(self, start_dt):
        """计算依赖 tag 列表"""

        tags = list()
        for item in json.loads(self.dependence):
            task_name = item["task_name"]
            kwargs = dict(zip(["days", "hours", "minutes"], item["offset"]))
            depend_task_start_dt = start_dt + datetime.timedelta(**kwargs)
            tags.append(self._get_tag_name(task_name, depend_task_start_dt, item["exec_unit"]))
        return tags

    def _get_plan_dt(self, end_dt):
        """计算任务计划执行时间，计划执行时间 = 任务执行区间右边界 + 执行延迟"""

        return end_dt + datetime.timedelta(minutes=self.delay)

    def _get_plan_expire_dt(self, end_dt):
        """计算任务计划执行超时时间，仅对循环执行任务有效。计划执行超时时间 = 任务执行区间右边界 + 执行延迟 + 启动超时"""

        return end_dt + datetime.timedelta(minutes=self.delay + self.start_expire)


if __name__ == '__main__':
    from datetime import timedelta
    import datetime
    import time

    # td = timedelta(days=0, seconds=create_dt.second, microseconds=create_dt.microsecond, milliseconds=0,
    #                minutes=create_dt.minute,
    #                hours=create_dt.hour, weeks=0)
    # new_dt = create_dt - td
    # start_dt = datetime.datetime.fromtimestamp(new_dt.timestamp())
