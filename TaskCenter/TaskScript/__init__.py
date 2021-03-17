__all__ = ["BaseTaskScript"]


class BaseTaskScript(object):
    """任务脚本基类"""

    def run_task(self, **kwargs):
        """执行任务"""

        raise NotImplementedError

    def run_success_callback(self, **kwargs):
        """成功回调"""

        # todo: 使用 logging 记录日志
        interval = kwargs.get("interval")
        task_batch_name = kwargs.get("task_batch_name")

    def run_failure_callback(self, **kwargs):
        """失败回调"""

        # todo: 使用 logging 记录日志
        error = kwargs.get("error")
        interval = kwargs.get("interval")
        task_batch_name = kwargs.get("task_batch_name")
