-- truncate table daily_stock;
-- alter sequence auto_add start 1;

-- DROP table daily_stock;
-- 复制表的结构和约束
-- create table daily_stock1 (like daily_stock);
-- 增加自增id序列
-- create SEQUENCE id_add START 1;
-- alter table daily_stock1 alter column id set default nextval('id_add');
-- 插入唯一约束
-- ALter table daily_stock add constraint u_daily_code unique (date,code);
-- 删除约束
-- alter table daily_stock drop constraint if EXISTS u_daily_code;

-- 根据date和code检查是否有重复插入
-- SELECT 
-- date,count(date)
-- code,count(code)
-- FROM
-- daily_stock
-- GROUP by
-- date,code
-- HAVING (count(date)>1) and (count(code)>1);

-- 使用窗口函数查找重复记录
-- SELECT date, name, code, count(*) over (partition by date,code) cnt
-- FROM daily_stock WHERE date='2023-05-22' order by cnt DESC;

-- -- 可读性更高的窗口函数
-- with d as 
-- (SELECT date, name, code, count(*) over (partition by date,code) cnt 
-- from daily_stock)
-- SELECT * from d WHERE cnt > 1;



-- 删除重复记录,主要是选择使用窗口函数分组的新表中出现row_num>1的id的记录
-- DELETE FROM daily_stock
-- where id in (
--     SELECT id from
--     (SELECT id, date, name, code, row_number() 
-- over(PARTITION by date,code order by id) as row_num
-- from daily_stock) d
-- WHERE row_num>1
-- );

-- 查看当前运行的pid
-- SELECT
--     procpid,
--     START,
--     now() - START AS lap,
--     current_query
-- FROM
--     (
--         SELECT
--             backendid,
--             pg_stat_get_backend_pid (S.backendid) AS procpid,
--             pg_stat_get_backend_activity_start (S.backendid) AS START,
--             pg_stat_get_backend_activity (S.backendid) AS current_query
--         FROM
--             (
--                 SELECT
--                     pg_stat_get_backend_idset () AS backendid
--             ) AS S
--     ) AS S
-- WHERE
--     current_query <> '<IDLE>'
-- ORDER BY
--     lap DESC;

