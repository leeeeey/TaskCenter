SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for task_batch
-- ----------------------------
DROP TABLE IF EXISTS `task_batch`;
CREATE TABLE `task_batch` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `task_name` varchar(255) NOT NULL DEFAULT '' COMMENT '任务名称',
  `task_tag_name` varchar(255) NOT NULL DEFAULT '' COMMENT 'tag 名称',
  `task_batch_name` varchar(255) NOT NULL DEFAULT '' COMMENT '批次名称',
  `exec_status` int(11) NOT NULL DEFAULT '0' COMMENT '批次执行状态',
  `dependence` text COMMENT '任务依赖',
  `start_time` varchar(255) NOT NULL DEFAULT '' COMMENT '时间片左边界',
  `end_time` varchar(255) NOT NULL DEFAULT '' COMMENT '时间片右边界',
  `plan_time` varchar(255) NOT NULL DEFAULT '' COMMENT '计划执行时间',
  `plan_expire_time` varchar(255) NOT NULL DEFAULT '' COMMENT '启动超时时间',
  `exec_time` varchar(255) NOT NULL DEFAULT '' COMMENT '开始执行时间',
  `exit_time` varchar(255) NOT NULL DEFAULT '' COMMENT '结束执行时间',
  `duration` int(11) NOT NULL DEFAULT '0' COMMENT '执行耗时',
  `retry` int(11) NOT NULL DEFAULT '0' COMMENT '已重试次数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=91228 DEFAULT CHARSET=utf8mb4 COMMENT='任务批次表';

SET FOREIGN_KEY_CHECKS = 1;
