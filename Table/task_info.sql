SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for task_info
-- ----------------------------
DROP TABLE IF EXISTS `task_info`;
CREATE TABLE `task_info` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `task_num` varchar(255) NOT NULL DEFAULT '' COMMENT '任务编号',
  `task_name` varchar(255) NOT NULL DEFAULT '' COMMENT '任务名称',
  `task_type` int(11) NOT NULL DEFAULT '0' COMMENT '任务类型',
  `online` int(11) NOT NULL DEFAULT '0' COMMENT '启用状态',
  `dependence` text COMMENT '任务依赖',
  `script` varchar(255) NOT NULL DEFAULT '' COMMENT '执行脚本',
  `script_args` varchar(255) NOT NULL DEFAULT '' COMMENT '脚本参数',
  `exec_unit` varchar(255) NOT NULL DEFAULT '' COMMENT '执行周期',
  `exec_unit_param` int(11) NOT NULL DEFAULT '0' COMMENT '周期参数',
  `delay` int(11) NOT NULL DEFAULT '0' COMMENT '执行延迟',
  `start_expire` int(11) NOT NULL DEFAULT '0' COMMENT '启动超时',
  `retry_max_times` int(11) NOT NULL DEFAULT '0' COMMENT '最大重试次数',
  `run_expire` int(11) NOT NULL DEFAULT '0' COMMENT '运行超时',
  `create_time` varchar(255) NOT NULL DEFAULT '' COMMENT '创建时间',
  `update_time` varchar(255) NOT NULL DEFAULT '' COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=82 DEFAULT CHARSET=utf8mb4 COMMENT='任务信息表';

SET FOREIGN_KEY_CHECKS = 1;
