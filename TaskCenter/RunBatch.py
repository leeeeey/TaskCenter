import os
import sys
import json
import math
import time
import datetime
import importlib
import threading
import multiprocessing
from sqlalchemy.exc import SQLAlchemyError

from Config import BaseConfig
from Utils import BaseUtils
from Table import TaskBatch, TaskInfo
from . import LocalUtils
import common_logger


# 初始化日志工具
common_logger.init_logger(BaseConfig.path_log, 'common_logger', is_need_console=True)


class Batch(threading.Thread):
    """任务类，对应 task_exec 表中一项待执行任务"""

    def __init__(self, **kwargs):
        """初始化，kwargs 为待执行任务的信息"""

        # 任务初始化信息
        self.retry = kwargs.get("retry")
        self.record_id = kwargs.get("id")
        self.script = kwargs.get("script")
        self.task_type = kwargs.get("task_type")
        self.task_name = kwargs.get("task_name")
        self.task_tag_name = kwargs.get("task_tag_name")
        self.run_expire = kwargs.get("run_expire")
        self.script_args = kwargs.get("script_args")
        self.start_expire = kwargs.get("start_expire")
        self.task_batch_name = kwargs.get("task_batch_name")
        self.retry_max_times = kwargs.get("retry_max_times")
        self.thread_name = self.batch_num = f"{kwargs.get('batch_num')}"
        self.end_time = datetime.datetime.strptime(kwargs.get("end_time"), "%Y-%m-%d %H:%M:%S")
        self.start_time = datetime.datetime.strptime(kwargs.get("start_time"), "%Y-%m-%d %H:%M:%S")
        self.interval = LocalUtils.Interval(int(self.start_time.timestamp()), int(self.end_time.timestamp()))

        # 任务执行状态
        self.success = False

        # 任务执行函数
        self.run_task = None
        self.run_success_callback = None
        self.run_failure_callback = None

        # 线程初始化
        super().__init__(name=self.thread_name, daemon=True)

    def run(self):
        """
        任务执行入口函数，任务执行成功设置 self.success 属性
        为了动态配置重试次数并且获取异常堆栈，未使用 retrying 模块
        """

        self.get_task_script()
        interval = self.interval
        script_args = self.script_args
        task_batch_name = self.task_batch_name
        task_tag_name = self.task_tag_name
        common_logger.info(f'{task_batch_name}:开始执行')
        while True:
            try:
                if self.retry:
                    common_logger.info(f'{task_batch_name}:第{self.retry}次重试')
                self.run_task(interval=interval, script_args=script_args, task_tag_name=task_tag_name)
                self.run_success_callback(interval=interval, task_batch_name=task_batch_name)
                self.success = True
                common_logger.info(f'{task_batch_name}:执行成功')
                break
            except Exception as e:
                try:
                    self.run_failure_callback(
                        interval=interval, task_batch_name=task_batch_name, error=e
                    )
                    common_logger.error(f'{task_batch_name}执行失败:{e}')
                except Exception as e:
                    common_logger.error(e)


            # 将要开始的重试次数，首次执行不计算在内
            self.retry += 1
            if self.retry > self.retry_max_times:
                # 非循环任务失败，发送DC
                if self.task_type == 0:
                    #
                    BaseUtils.err_to_dc(task_batch_name)
                break
            self.update_record(retry=self.retry)
            time.sleep(5)

    def get_task_script(self):
        """
        设置任务执行使用的函数
        必须需要提供 run_task、run_success_callback、run_failure_callback 三个函数
        函数接收可变关键字参数 **kwargs，传入参数由 self.run 中定义，至少包含描述任务执行时间区间 interval
        函数存放于 self.task_name 属性同名脚本，脚本存储于 /path/to/project/TaskCenter/TaskScript 目录下
        """

        sys.path.append(os.path.abspath(f"{ BaseConfig.path_project}/TaskCenter/TaskScript"))
        script_module = importlib.import_module(self.script)
        script_obj = script_module.Script()
        self.run_task = script_obj.run_task
        self.run_success_callback = script_obj.run_success_callback
        self.run_failure_callback = script_obj.run_failure_callback

    def update_record(self, **kwargs):
        """更新任务信息"""

        session_w = BaseUtils.init_mysql_session("w")
        t = TaskBatch.TaskBatch
        try:
            session_w.query(t).filter_by(id=self.record_id).update(kwargs)
            session_w.commit()
        except SQLAlchemyError as e:
            session_w.rollback()
            common_logger.error(f'{json.dumps(kwargs)}数据提交失败:{e}')
            raise e


class TaskManager(object):
    """任务管理器"""

    def __init__(self, task_num):
        """
        初始化
        :param task_num: 同时执行的任务数量
        """
        # 初始化logging,注意日志目录要存在

        self.task_num = task_num
        self.task_kwargs_list = list()
        self.exec_time = datetime.datetime.now()

    def get_ready_task(self):
        """获取待执行任务"""

        # 初始化
        session_w = BaseUtils.init_mysql_session("w")
        now = self.exec_time.strftime("%Y-%m-%d %H:%M:%S")

        # 记录参数，但不直接初始化 Task 对象，避免多进程传参时，因为继承 Thread 类，Task 无法被 pickle 模块序列化的问题
        task_kwargs_list = self.task_kwargs_list
        try:
            # 区分预发、生产的batch
            batch_infos = session_w.query(TaskInfo.TaskInfo.task_name).filter(
                TaskInfo.TaskInfo.online == BaseConfig.ENV_TYPE).all()
            run_batch_list = [batch_info[0] for batch_info in batch_infos]
            common_logger.info(f'待执行任务数：{len(run_batch_list)}')
            # 加锁查询
            t = TaskBatch.TaskBatch
            records = session_w.query(t).filter(
                t.exec_status.in_((0, 1)) & (t.plan_time <= now) & (t.task_name.in_(run_batch_list))).order_by(
                t.plan_time) \
                .with_for_update().all()
            ready_batch_list = list()
            for record in records:
                if len(ready_batch_list) == self.task_num:
                    break
                # 循环任务失败判定，发送DC报警
                if record.exec_status == 1 and record.plan_expire_time < now:
                    record.exec_status = -1
                    # todo：替换告警函数
                    BaseUtils.err_to_dc(record.task_batch_name)
                    continue
                dependence = json.loads(record.dependence)
                for tag in dependence:
                    b = session_w.query(t).filter_by(task_tag_name=tag).order_by(t.task_batch_name.desc()).first()
                    if b is None or b.exec_status not in (3, 4):
                        break
                else:
                    ready_batch_list.append(record)
            common_logger.info(f'符合执行条件任务数：{len(ready_batch_list)}')
            # 初始化 Task 对象，进入待执行状态，返回 Task 对象列表
            t = TaskInfo.TaskInfo
            for record in ready_batch_list:
                task_info = session_w.query(t).filter_by(task_name=record.task_name).one()
                record.exec_status, record.exec_time = 2, now
                task_kwargs = record.to_dict()
                task_kwargs.update(
                    retry_max_times=task_info.retry_max_times,
                    run_expire=task_info.run_expire,
                    task_type=task_info.task_type,
                    script=task_info.script,
                    script_args=task_info.script_args
                )
                task_kwargs_list.append(task_kwargs)
            session_w.commit()
        except SQLAlchemyError as e:
            session_w.rollback()
            common_logger.error(f'获取任务时, 修改任务状态失败:{e}')
            raise e

        return task_kwargs_list

    def execute_task_once(self, **kwargs):
        """
        执行任务，多进程目标函数
        :param kwargs: Task 对象初始化参数
        """

        # 任务执行
        task = Batch(**kwargs)
        task.start()
        task.join(task.run_expire * 60)

        # 更新执行状态
        exit_time = datetime.datetime.now()
        duration = math.ceil((exit_time.timestamp() - self.exec_time.timestamp()) / 60)

        # # 执行成功
        if task.success:
            kwargs = dict(
                exec_status=3,
                duration=duration,
                exit_time=exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            )
        # # 执行失败，且需要循环执行的任务，初始化相关状态
        elif task.task_type == 1:
            kwargs = dict(
                retry=0,
                duration=0,
                exec_status=1,
                exec_time="0000-00-00 00:00:00",
                exit_time=exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            )
        # # 线程未结束，认为超时，随主线程结束退出，因为先判断超时再退出线程，存在标识任务超时但正常执行完毕的微小可能
        # # 超时执行失败
        elif task.is_alive():
            kwargs = dict(
                exec_status=-2,
                duration=duration,
                exit_time=exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            )
        # # 出现异常执行失败
        else:
            kwargs = dict(
                exec_status=-1,
                duration=duration,
                exit_time=exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            )
        task.update_record(**kwargs)

    def execute_task(self):
        """执行任务，多进程入口函数"""

        # 关闭父进程创建的数据库连接，保证数据库连接使用进程安全
        session_r = BaseUtils.init_mysql_session("r")
        session_w = BaseUtils.init_mysql_session("w")
        session_r.bind.dispose()
        session_w.bind.dispose()

        pool = multiprocessing.Pool(self.task_num)
        for task_kwargs in self.task_kwargs_list:
            pool.apply_async(self.execute_task_once, kwds=task_kwargs, error_callback=self.handle_error)
        pool.close()
        pool.join()

    def handle_error(self, error):
        """多进程异常回调"""

        # todo: 记录异常日志
        pass


@common_logger.logging_wrapper
def run():
    """功能入口函数"""
    # 查询 cpu 数量，决定同时执行的任务数量
    cpu_count = multiprocessing.cpu_count()
    common_logger.info(f'共开启{cpu_count}个进程.')
    task_manager = TaskManager(cpu_count)
    batch_count = task_manager.get_ready_task()
    common_logger.info(f'共{len(batch_count)}个批次任务待执行.')
    task_manager.execute_task()
    # task_manager.execute_task_once(**task_manager.task_kwargs_list[0])


if __name__ == '__main__':
    run()
