import time
import datetime

from Utils import BaseUtils
from TaskCenter import TaskScript
from Table import TaskInfo, TaskBatch
from .. import LocalUtils
from Config.BaseConfig import ENV_TYPE
from Utils.LogUtils import common_logger


class Script(TaskScript.BaseTaskScript):
    """任务脚本基类"""

    def __init__(self):
        """初始化"""

        self.session_w = BaseUtils.init_mysql_session("w")

    def run_task(self, **kwargs):
        """执行任务"""

        session_w = self.session_w
        interval = kwargs.get("interval")
        current_dt = datetime.datetime.fromtimestamp(interval.ts_end)

        t = TaskInfo.TaskInfo
        try:
            info_list = session_w.query(t).filter_by(online=ENV_TYPE).with_for_update().all()
            for info in info_list:
                t = TaskBatch.TaskBatch
                task_name = info.task_name
                last_batch = session_w.query(t).filter_by(task_name=task_name).order_by(t.task_tag_name.desc()).first()
                if last_batch:
                    last_start_dt = datetime.datetime.strptime(last_batch.start_time, "%Y-%m-%d %H:%M:%S")
                    next_start_dt = info.get_next_start_dt(last_start_dt)
                else:
                    next_start_dt = info.get_init_start_dt(current_dt)
                while True:
                    if next_start_dt > current_dt + datetime.timedelta(hours=3):
                        break
                    task = info.create_new_task(next_start_dt, 1)
                    session_w.add(task)
                    next_start_dt = info.get_next_start_dt(next_start_dt)
            session_w.commit()
        except Exception as e:
            session_w.rollback()
            raise e


def first_run():
    """批次表无记录时，手动创建，用于初始化"""

    script = Script()
    t = TaskBatch.TaskBatch
    record = script.session_w.query(t).with_for_update().first()
    if not record:
        current_ts = int(time.time())
        interval = LocalUtils.Interval(current_ts, current_ts)
        script.run_task(interval=interval)


if __name__ == '__main__':
    first_run()
