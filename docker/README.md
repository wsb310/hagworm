# Docker部署文档


## 1.服务器操作系统

建议安装CentOS7操作系统，本文是基于CentOS7的安装指南


## 2.服务器基本环境

更新系统，禁用SELinux。未更新系统或者开启SELinux可能会影响Docker的安装和运行。

### 更新系统
    yum -y update

### 查看SELinux状态
    sestatus

### 禁用SELinux
    sed -i '/SELINUX/s/enforcing/disabled/' /etc/selinux/config

### 重启机器
    shutdown -r now

### 确认SELinux状态
    sestatus

### 安装epel支持
    yum install -y epel-release

### 修改服务器时区
    timedatectl set-timezone Asia/Shanghai

### 同步服务器时间
    yum install -y ntpdate
    ntpdate us.pool.ntp.org


## 3.Docker安装

Docker-compose需要Python 2.7以上版本支持

### 确认Python安装
    python -V

### 安装pip支持
    yum install -y python-pip

### 安装Docker环境
    yum install -y docker

### 注册Docker服务
    service docker start
    chkconfig docker on

### 安装Docker-compose环境
    pip install -U docker-compose

### 注册国内镜像
    vi /etc/sysconfig/docker
    OPTIONS='--registry-mirror=http://xxx.m.daocloud.io --selinux-enabled'


## 4.Docker部署

部署完成Docker文件后，注意所有sh文件和映射的文件夹需要至少有755权限，否则可能造成如数据库启动失败、计划任务无法执行等权限引起的问题。
