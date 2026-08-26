基于 Go 实现的煤矿井下瓦斯监测与通风联锁系统项目，一款矿山安全设备控制服务，完成井下多测点瓦斯浓度监测、超限报警、局部通风机联动与断电闭锁管理。

## 构建与运行

```bash
go build -mod=vendor -o coalminegas.exe ./cmd/server
./coalminegas.exe -addr=:8080
```

启动后访问 http://127.0.0.1:8080/ 打开井上集控台页面，页面通过 /api/console/summary 获取实时汇总。

## 接口

- GET /api/console/summary 汇总状态
- POST /api/console/start-fan 启动局部通风机
- POST /api/console/trigger-trip 触发断电闭锁
- POST /api/console/reset-trip 复位闭锁
- POST /api/console/restore 现场手动复电
- POST /api/console/stop-after-stable 浓度回稳后停风机
- POST /api/console/calibrate 测点标校
- GET /api/console/stream 状态流

## 前端

web/console.html 为井上监控页面，包含瓦斯浓度、报警与风机状态展示。
