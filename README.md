# Hagworm

![](https://img.shields.io/pypi/v/hagworm.svg)
![](https://img.shields.io/pypi/format/hagworm.svg)
![](https://img.shields.io/pypi/implementation/hagworm.svg)
![](https://img.shields.io/pypi/pyversions/hagworm.svg)
![](https://img.shields.io/github/license/wsb310/hagworm.svg)
![](https://img.shields.io/github/languages/code-size/wsb310/hagworm.svg)
![](https://img.shields.io/github/repo-size/wsb310/hagworm.svg)
![](https://img.shields.io/github/downloads/wsb310/hagworm/total.svg)
![](https://img.shields.io/github/forks/wsb310/hagworm.svg)
![](https://img.shields.io/github/stars/wsb310/hagworm.svg)
![](https://img.shields.io/github/watchers/wsb310/hagworm.svg)
![](https://img.shields.io/github/last-commit/wsb310/hagworm.svg)

## 快速开始

### 1. 下载

```bash
git clone git@github.com:wsb310/hagworm.git
```

### 2. 安装

```bash
pip install hagworm
```

### 3. 代码树结构

```text
├── extend                                      基础扩展
│   ├── asyncio                                 asyncio扩展
│   │   ├── base.py                             异步工具库
│   │   ├── cache.py                            缓存相关
│   │   ├── database.py                         数据库相关
│   │   ├── event.py                            分布式事件总线
│   │   ├── file.py                             文件读写相关
│   │   ├── future.py                           协程相关
│   │   ├── net.py                              网络工具
│   │   └── task.py                             任务相关
│   ├── base.py                                 基础工具
│   ├── compile.py                              pyc编译
│   ├── crypto.py                               加解密相关
│   ├── event.py                                事件总线
│   ├── excel.py                                excel封装
│   ├── interface.py                            接口定义
│   ├── metaclass.py                            元类相关
│   └── struct.py                               数据结构
├── frame                                       三方框架扩展
│   └── tornado                                 tornado扩展
│       ├── base.py                             http基础工具
│       ├── socket.py                           socket基础工具
│       └── web.py                              http请求处理工具
├── static                                      静态资源
│   └── cacert.pem                              SSL根证书
└── third                                       三方库扩展
```

### 4. 重要提示

* 不要在非异步魔术方法中调用异步函数，例如在__del__中调用异步函数，在该函数结束前，对象一直处于析构中的状态，此时弱引用是有效的，但如果此时另外一个线程或者协程通过弱引用去使用它，然后意外就可能发生了
* 使用contextvars库时，要注意使用asyncio的call_soon、call_soon_threadsafe、call_later和call_at函数时（建议使用hagworm.extend.asyncio.base.Utils提供的函数），其中的context参数，必须给出独立的contextvars.Context对象，使其上下文环境独立，否则会出现伪内存泄漏现象
