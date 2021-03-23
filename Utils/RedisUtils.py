"""
Usage：
from Utils import BaseUtils

# 1. 优先使用上下文管理器，默认阻塞
redis_cli = BaseUtils.init_redis_client()
with redis_cli.lock(name=lock_name, timeout=60, blocking_timeout=5) as lock:
    pass

# 2. 可以直接操作锁的获取和释放，默认不阻塞
lock = redis_cli.lock(name=lock_name, timeout=60, blocking_timeout=5)
lock.acquire(blocking=True)
try:
    pass
finally:
    lock.release()
"""
import hashlib
import redis.lock
import common_logger
from Config.BaseConfig import path_log

common_logger.init_logger(path_log, 'threatintel', is_need_console=True, backupCount=10,
                               rotate_type='MIDNIGHT')


class RedisLock(redis.lock.Lock):
    """重写锁对象，修改 lua 脚本的调用方式"""

    extend_script_sha1 = hashlib.sha1(redis.lock.Lock.LUA_EXTEND_SCRIPT.encode()).hexdigest()
    release_script_sha1 = hashlib.sha1(redis.lock.Lock.LUA_RELEASE_SCRIPT.encode()).hexdigest()
    reacquire_script_sha1 = hashlib.sha1(redis.lock.Lock.LUA_REACQUIRE_SCRIPT.encode()).hexdigest()

    def register_scripts(self):
        """不再注册脚本，依赖 eval 命令执行后自动注册"""

        pass

    def do_extend(self, additional_time, replace_ttl):
        """延长锁"""

        additional_time = int(additional_time * 1000)
        try:
            resp = self.redis.evalsha(
                self.extend_script_sha1, 1, self.name, self.local.token, additional_time, replace_ttl and "1" or "0"
            )
        except redis.exceptions.NoScriptError:
            resp = self.redis.eval(
                self.LUA_EXTEND_SCRIPT, 1, self.name, self.local.token, additional_time, replace_ttl and "1" or "0"
            )

        if not bool(resp):
            raise redis.lock.LockNotOwnedError("Cannot extend a lock that's no longer owned")
        return True

    def do_release(self, expected_token):
        """释放锁"""

        try:
            resp = self.redis.evalsha(self.release_script_sha1, 1, self.name, expected_token)
        except redis.exceptions.NoScriptError:
            resp = self.redis.eval(self.LUA_RELEASE_SCRIPT, 1, self.name, expected_token)

        if not bool(resp):
            raise redis.lock.LockNotOwnedError("Cannot release a lock that's no longer owned")

    def do_reacquire(self):
        """重入锁"""

        timeout = int(self.timeout * 1000)
        try:
            resp = self.redis.evalsha(self.reacquire_script_sha1, 1, self.name, self.local.token, timeout)
        except redis.exceptions.NoScriptError:
            resp = self.redis.eval(self.LUA_REACQUIRE_SCRIPT, 1, self.name, self.local.token, timeout)

        if not bool(resp):
            raise redis.lock.LockNotOwnedError("Cannot reacquire a lock that's no longer owned")
        return True


class Redis(redis.StrictRedis):
    """重写客户端对象，修改默认的锁对象"""

    def lock(self, name, timeout=None, sleep=0.1, blocking_timeout=None, lock_class=RedisLock, thread_local=True):
        """
        生成锁对象
        :param name: 锁名称
        :param timeout: 锁超时时间（秒）
        :param sleep: 循环检查周期（秒）
        :param blocking_timeout: 阻塞超时时间（秒）
        :param lock_class: 锁对象
        :param thread_local: 是否使用本地线程
        :return: 锁对象
        """

        name = f"lock:{name}"

        return super().lock(name, timeout, sleep, blocking_timeout, lock_class, thread_local)

    def execute_command(self, *args, **options):
        # log
        common_logger.info(f'{args[0],args[1]},options:{options}')
        res = super(Redis, self).execute_command(*args, **options)
        common_logger.info('resdis response:'+str(res))
        return res


if __name__ == '__main__':
    import os
    os.getenv("DIDIENV_DDCLOUD_ENV_TYPE")
