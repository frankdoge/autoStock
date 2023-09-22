-- 创建一个股票订单模版
create table StockOrder
(
    -- 主键
    id SERIAL Primary key, 
    -- 股票代码
    code varchar(30),
    -- 股票名称
    name varchar (16),
    -- 股票价格
    price numeric(10,3),
    -- 买入条件(1/2/3分别代表固定价格，价格比例，均线)
    buy_percent int,
    -- 卖出条件
    sell_percent int,
    -- 订单状态
    state int,
    -- 创建时间
    created_time date,
    -- 更新时间
    updated_time date,
    -- 买入价格
    buy_price numeric(10,3),
    -- 卖出价格
    sell_price numeric(10,3),
    -- 买入数量
    amount int,
    -- 买入策略参数，
    stra_param_buy varchar(50),
    -- 卖出策略参数，
    stra_param_sell varchar(50)
);
