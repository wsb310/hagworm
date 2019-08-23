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
├── extend                                      package     基础扩展
│   ├── asyncio                                 package     asyncio扩展
│   │   ├── base.py
│   │   ├── cache.py
│   │   ├── database.py
│   │   ├── event.py
│   │   ├── file.py
│   │   ├── future.py
│   │   └── net.py
│   ├── base.py
│   ├── compile.py
│   ├── crypto.py
│   ├── event.py
│   ├── excel.py
│   ├── interface.py
│   ├── metaclass.py
│   └── struct.py
├── frame                                       package     三方框架扩展
│   └── tornado
│       ├── base.py
│       ├── cache.py
│       ├── future.py
│       ├── template.py
│       └── web.py
└── static                                      package     静态资源
```

### 4. 重要提示

* 不要在非异步魔术方法中调用异步函数，例如在__del__中调用异步函数，在该函数结束前，对象一直处于析构中的状态，此时弱引用是有效的，但如果此时另外一个线程或者协程通过弱引用去使用它，然后意外就可能发生了
* 使用contextvars库时，要注意使用asyncio的call_soon、call_soon_threadsafe、call_later和call_at函数时（建议使用hagworm.extend.asyncio.base.Utils提供的函数），其中的context参数，必须给出独立的contextvars.Context对象，使其上下文环境独立，否则会出现伪内存泄漏现象
