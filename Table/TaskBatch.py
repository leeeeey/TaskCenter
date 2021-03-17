from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, Column, String, Text

Base = declarative_base()


class TaskBatch(Base):
    __tablename__ = 'task_batch'

    id = Column(Integer, primary_key=True)
    task_name = Column(String(255))
    task_tag_name = Column(String(255))
    task_batch_name = Column(String(255))
    exec_status = Column(Integer)
    dependence = Column(Text)
    start_time = Column(String(255))
    end_time = Column(String(255))
    plan_time = Column(String(255))
    plan_expire_time = Column(String(255))
    exec_time = Column(String(255))
    exit_time = Column(String(255))
    duration = Column(Integer)
    retry = Column(Integer)

    def to_dict(self):
        """转换为 dict 类型"""

        return dict(
            id=self.id,
            task_name=self.task_name,
            task_tag_name=self.task_tag_name,
            task_batch_name=self.task_batch_name,
            exec_status=self.exec_status,
            dependence=self.dependence,
            start_time=self.start_time,
            end_time=self.end_time,
            plan_time=self.plan_time,
            plan_expire_time=self.plan_expire_time,
            exec_time=self.exec_time,
            exit_time=self.exit_time,
            duration=self.duration,
            retry=self.retry,
        )


if __name__ == '__main__':
    pass
