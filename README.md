# MIT6.824

MIT6.824课程学习

* 关于多线程和并发的一些基础知识

对于worker而言，它的工作只是处理coordinator分配给它的任务即可，所以它不需要进行多线程操作，完成一个单线程的串行任务即可

对于coordinator而言，它需要维护一个主线程（goroutine），每收到一个worker发来的请求，就开启一个goroutine进行并行处理

* 待学习任务

1. go chanel
2. shell脚本