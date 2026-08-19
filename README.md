# minirpc
一个从零实现的轻量级 C++ RPC 框架。
## 1.简介
包含客户端、服务器和注册中心三大组件：

- **服务端**:服务的提供方，负责执行本地业务逻辑并返回结果；
- **客户端**:服务的调用方，发起远程调用请求；
- **注册中心**:服务注册与发现；

组件间间的数据交互通过TCP网络传输实现，其数据包封装方式：自定义数据头 + protobuf序列化的业务数据。自定义数据头携带数据长度等信息，解决TCP粘包/半包问题，protobuf序列化则能够有效减少传输的总数据量。
## 2.效果显示
基础使用效果如下，客户端询问注册中心获取服务节点，以同步/异步方式访问服务节点上的echo服务，获取最终响应。
<img width="1055" height="192" alt="image" src="https://github.com/user-attachments/assets/ef9ccf9d-1696-47a9-be1f-a3e2bdd6eb82" />

在本机环境下测试(Windows的WSL2子系统)，一定数据量下，客户端不断访问echo服务，较理想条件下QPS约**4w**请求/s。
<img width="1398" height="408" alt="image" src="https://github.com/user-attachments/assets/fced7435-b876-40c0-b10c-8addf1f72ff7" />
