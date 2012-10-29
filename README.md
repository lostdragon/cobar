cobar
=====

Based on alibaba cobar 1.2.6 modifications.

* 分库表名增加正则支持，正则中需要包含[]，因“,”是正则关键字，移除,分隔表；
* 分库规则增加根据ID模切分；
* 启动参数中Xss128k 修改为 Xss256k；
