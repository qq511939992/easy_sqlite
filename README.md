# easy_sqlite

这是一个简单的sqlite学习记录，源码和教程来自于[cstack](https://github.com/cstack)，自己在源码的基础上增加了很多易读的注释，感兴趣的小伙伴可以看一下。

**说明**：本源码主要实现了数据库的两个功能，insert和select，且只支持单数据表，使用B-tree结构存放数据，目前只能实现两层树结构。

insert：格式硬编码在代码中。

select：只支持全部查询，不支持条件查询。
