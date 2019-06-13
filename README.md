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
