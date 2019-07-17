# FactorCalculate
RL 因子计算

# 1、目录架构
    .
    ├── client  # 客户端任务执行入口
    │   ├── __init__.py
    │   ├── all_factor_cal.py  # 所有因子合并计算
    │   ├── cash_flow.py
    │   ├── constrain.py
    │   ├── earning.py
    │   ├── factor_scale_value.py
    │   ├── factor_volatility_value.py  
    │   ├── growth.py
    │   ├── historical_value.py
    │   └── per_share_indicator.py
    ├── factor  # 因子计算任务列表
    │   ├── __init__.py
    │   ├── factor_cash_flow.py              # 收益质量
    │   ├── factor_constrain.py              # 收益质量
    │   ├── factor_earning.py                # 收益质量
    │   ├── factor_per_share_indicators.py   # 收益质量
    │   ├── factor_growth.py                 # 历史成长
    │   ├── factor_scale_value_task.py       # 规模
    │   ├── factor_volatility_value_task.py  # 波动 
    │   ├── historical_value.py              # 价值
    │   ├── factor_base.py  # 基类
    │   ├── factor_config.py  # 因子计算配置文件，包括数据读取地址， 存储地址等
    │   ├── ttm_fundamental.py  # TTM转换类
    │   └── utillities  # 工具类
    ├── README.md
    ├── cluster_work.py
    ├── init.py
    └── sumbit.py

# 2、细节说明
### /factor
该文件目录下保存的是因子计算的task文件， 以及每个task所依赖的因子计算文件。分布式计算时， 该目录下面的所有文件会提交到每个节点。

### /client
程序执行入口, 文件目录下包含单类因子计算， 以及合并计算
##### 使用示例
```shell
# 更新
python ./client/earning.py --end_date 20190101  --count 3 --update True
python ./client/all_factor_cal.py --end_date 20190101  --count 3 --update True
```
具体参见client中的每个客户端代码。

### cluster_work
分布式引擎节点启动程序，需后台运行。

### init
初始化分布式计算redis，运行之前需要在文件中设定需要指定的redis信息。

### sumbit
分布式计算任务提交入口，执行之后，客户端会将指定目录即目录下所有文件分发到所有计算节点中。不同的客户端在提交任务时需要修改其中的相关配置。

