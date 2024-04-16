--выполняем перенос данных из источника в хранилище

/* создание таблицы tmp_sources_external c новыми данными из внешних источников */
drop table if exists tmp_sources_external;
create temp table tmp_sources_external as
select 
	cpo.craftsman_id,
	cpo.craftsman_name,
	cpo.craftsman_address,
	cpo.craftsman_birthday,
	cpo.craftsman_email,
	
	c.customer_id,
	c.customer_name,
	c.customer_address,
	c.customer_birthday,
	c.customer_email,
	
	cpo.product_id,
	cpo.product_name,
	cpo.product_description,
	cpo.product_type,
	cpo.product_price,
	
	cpo.order_id,
	cpo.order_created_date,
	cpo.order_completion_date,
	cpo.order_status
	
from external_source.craft_products_orders cpo 
join external_source.customers c
using (customer_id);

/* создание таблицы tmp_sources_external_fact */
DROP TABLE IF EXISTS tmp_sources_external_fact;
CREATE TEMP TABLE tmp_sources_external_fact AS 
SELECT  dp.product_id,
        dc.craftsman_id,
        dcust.customer_id,
        tse.order_created_date,
        tse.order_completion_date,
        tse.order_status,
        current_timestamp 
FROM tmp_sources_external tse
JOIN dwh.d_craftsman dc 
	ON dc.craftsman_name = tse.craftsman_name 
		and dc.craftsman_email = tse.craftsman_email 
JOIN dwh.d_customer dcust 
	ON dcust.customer_name = tse.customer_name 
		and dcust.customer_email = tse.customer_email 
JOIN dwh.d_product dp 
	ON dp.product_name = tse.product_name 
	and dp.product_description = tse.product_description 
	and dp.product_price = tse.product_price;

/* обновление существующих записей и добавление новых в dwh.d_craftsmans */
merge into dwh.d_craftsman cr
using (select distinct craftsman_name, craftsman_address, craftsman_birthday, craftsman_email from tmp_sources_external) t
	on cr.craftsman_name = t.craftsman_name
		and cr.craftsman_email = t.craftsman_email
when matched then 
update set craftsman_address = t.craftsman_address,
craftsmanbirthday = t.craftsman_birthday,
load_dttm = current_timestamp
when not matched then 
insert (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_product */
merge into dwh.d_product p
using (select distinct product_name, product_description, product_type, product_price from tmp_sources_external) t
	ON p.product_name = t.product_name 
 AND p.product_description = t.product_description 
 AND p.product_price = t.product_price
when matched then 
UPDATE SET product_type = t.product_type
, load_dttm = current_timestamp
when not matched then 
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);
 
/* обновление существующих записей и добавление новых в dwh.d_customer */
MERGE INTO dwh.d_customer cu
USING (SELECT DISTINCT customer_name, customer_address, customer_birthday, customer_email from tmp_sources) t
ON cu.customer_name = t.customer_name 
AND cu.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET customer_address = t.customer_address, 
	customer_birthday = t.customer_birthday, 
	load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
  VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.f_order */
MERGE INTO dwh.f_order f
USING tmp_sources_external_fact t
ON f.product_id = t.product_id 
 AND f.craftsman_id = t.craftsman_id 
 AND f.customer_id = t.customer_id 
 AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET order_completion_date = t.order_completion_date, 
  order_status = t.order_status, 
  load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp);
  
 --создаем ddl витрины данных

drop table if exists dwh.customer_report_datamart;
create table if not exists dwh.customer_report_datamart (
id BIGINT GENERATED ALWAYS AS IDENTITY NOT null, --идентификатор записи
customer_id bigint not null, --идентификатор заказчика
customer_name varchar not null, --Ф. И. О. заказчика
customer_address varchar not null, --адрес заказчика
customer_birthday date not null, --дата рождения заказчика
customer_email varchar not null, --электронная почта заказчика
customer_money bigint NOT NULL, --сумма, которую потратил заказчик
platform_money bigint NOT NULL, --сумма, которую заработала платформа от покупок заказчика за месяц
count_order bigint not null, --количество заказов у заказчика за месяц
avg_price_order bigint not null, --средняя стоимость одного заказа у заказчика за месяц
median_time_order_completed int not null, --медианное время в днях от момента создания заказа до его завершения за месяц
top_product_category varchar not null, --самая популярная категория товаров у этого заказчика за месяц
top_master_id bigint not null, --идентификатор самого популярного мастера ручной работы у заказчика
count_order_created bigint not null, --количество созданных заказов за месяц
count_order_in_progress bigint not null, --количество заказов в процессе изготовки за месяц
count_order_delivery bigint not null, --количество заказов в доставке за месяц
count_order_done bigint not null, --количество завершённых заказов за месяц
count_order_not_done bigint not null, --количество незавершённых заказов за месяц
report_period VARCHAR NOT null, --отчётный период, год и месяц
constraint customer_report_datamart_rk primary key (id)
);

--ddl таблицы инкрементальных загрузок
drop table if exists dwh.load_dates_customer_report_datamart;
create table if not exists dwh.load_dates_customer_report_datamart (
id bigint generated always as identity not null,
load_dttm date not null,
constraint load_dates_customer_report_datamart_pk primary key (id)
);

--инкрементальное обновление витрины

with 
/* определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем дельту изменений. */
dwh_delta AS (
    SELECT     
            dcs.customer_id AS customer_id,
            dcs.customer_name AS customer_name,
            dcs.customer_address AS customer_address,
            dcs.customer_birthday AS customer_birthday,
            dcs.customer_email AS customer_email,
            dc.craftsman_id AS craftsman_id,
            dp.product_id AS product_id,
            dp.product_price AS product_price,
            dp.product_type AS product_type,
            fo.order_id AS order_id,
            fo.order_completion_date - fo.order_created_date AS diff_order_date, 
            fo.order_status AS order_status,
            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
            crd.customer_id AS exist_customer_id,
            dcs.load_dttm AS customers_load_dttm,
            dc.load_dttm AS craftsman_load_dttm,
            dp.load_dttm AS products_load_dttm
            FROM dwh.f_order fo 
            	INNER JOIN dwh.d_customer dcs 
                	ON fo.customer_id = dcs.customer_id 
                INNER JOIN dwh.d_craftsman dc 
                	ON fo.craftsman_id = dc.craftsman_id 
                INNER JOIN dwh.d_product dp 
                	ON fo.product_id = dp.product_id 
                LEFT JOIN dwh.customer_report_datamart crd 
                	ON dc.customer_id = crd.customer_id
                    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),

/* -- делаем выборку заказчиков, по которым были изменения в DWH. По ним данные в витрине нужно будет обновить */
dwh_update_delta AS (
    SELECT     
            dd.exist_customer_id AS customer_id
            FROM dwh_delta dd 
                WHERE dd.exist_customer_id IS NOT NULL        
),

/* Делаем расчёт витрины по новым данным */
dwh_delta_insert_result AS (
    SELECT  
            T5.customer_id AS craftsman_id,
            T5.customer_name AS craftsman_name,
            T5.customer_address AS craftsman_address,
            T5.customer_birthday AS craftsman_birthday,
            T5.customer_email AS craftsman_email,
            T5.customer_money AS craftsman_money,
            T5.platform_money AS platform_money,
            T5.count_order AS count_order,
            T5.avg_price_order AS avg_price_order,
            T5.median_time_order_completed AS median_time_order_completed,
            T5.product_type AS top_product_category,
            T5.craftsman_id AS top_craftsman,
            T5.count_order_created AS count_order_created,
            T5.count_order_in_progress AS count_order_in_progress,
            T5.count_order_delivery AS count_order_delivery,
            T5.count_order_done AS count_order_done,
            T5.count_order_not_done AS count_order_not_done,
            T5.report_period AS report_period 
            FROM (-- в этой выборке объединяем три внутренние выборки по расчёту столбцов витрины
                        *,
                        row_number() OVER(PARTITION BY T2.craftsman_id ORDER BY count_product_by_type DESC) AS rank_count_product_by_type,
                        row_number() OVER(PARTITION BY T2.craftsman_id ORDER BY count_product_by_master DESC) AS rank_count_product_by_master 
                        FROM ( 
                            SELECT-- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
                                T1.customer_id AS customer_id,
                                T1.customer_name AS customer_name,
                                T1.customer_address AS customer_address,
                                T1.customer_birthday AS customer_birthday,
                                T1.customer_email AS customer_email,
                                SUM(T1.product_price) AS customer_money,
                                SUM(T1.product_price) * 0.1 AS platform_money,
                                COUNT(order_id) AS count_order,
                                AVG(T1.product_price) AS avg_price_order,
                                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,                              
                                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
                                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
                                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
                                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                                T1.report_period AS report_period
                                FROM dwh_delta AS T1
                                    WHERE T1.exist_customer_id IS NULL
                                        GROUP BY T1.customer_id, 
                                        		 T1.customer_name, 
                                        		 T1.customer_address, 
                                        		 T1.customer_birthday, 
                                        		 T1.customer_email, 
                                        		 T1.report_period
                            ) AS T2 
                                INNER JOIN (
                                    SELECT --Определяем самую популярную категорию продукта у заказчика
                                            dd.customer_id AS customer_id_for_product_type, 
                                            dd.product_type, 
                                            COUNT(dd.product_id) AS count_product_by_type
                                            FROM dwh_delta AS dd
                                                GROUP BY dd.customer_id, 
                                                		dd.product_type
                                                    ORDER BY count_product_by_type DESC
                               ) AS T3 
                              ON T2.customer_id = T3.customer_id_for_product_type
                              INNER JOIN (
                                    SELECT --Определяем самого популярного мастера у заказчика
                                            dd.customer_id AS customer_id_for_top_master, 
                                            dd.craftsman_id, 
                                            COUNT(dd.product_id) AS count_product_by_master
                                            FROM dwh_delta AS dd
                                                GROUP BY dd.customer_id, 
                                                		 dd.craftsman_id
                                                    ORDER BY count_product_by_master DESC
                                ) AS T4 
                              ON T2.customer_id = T4.customer_id_for_top_master
                ) AS T5 
               WHERE T5.rank_count_product_by_type = 1 
               	AND T5.rank_count_product_by_master = 1
               ORDER BY report_period -- условие помогает оставить в выборке первую по популярности категорию товаров
),

dwh_delta_update_result AS (
    SELECT  
            T5.customer_id AS craftsman_id,
            T5.customer_name AS craftsman_name,
            T5.customer_address AS craftsman_address,
            T5.customer_birthday AS craftsman_birthday,
            T5.customer_email AS craftsman_email,
            T5.customer_money AS craftsman_money,
            T5.platform_money AS platform_money,
            T5.count_order AS count_order,
            T5.avg_price_order AS avg_price_order,
            T5.median_time_order_completed AS median_time_order_completed,
            T5.product_type AS top_product_category,
            T5.craftsman_id AS top_craftsman,
            T5.count_order_created AS count_order_created,
            T5.count_order_in_progress AS count_order_in_progress,
            T5.count_order_delivery AS count_order_delivery,
            T5.count_order_done AS count_order_done,
            T5.count_order_not_done AS count_order_not_done,
            T5.report_period AS report_period 
            FROM (-- в этой выборке объединяем три внутренние выборки по расчёту столбцов витрины
                        *,
                        row_number() OVER(PARTITION BY T2.craftsman_id ORDER BY count_product_by_type DESC) AS rank_count_product_by_type,
                        row_number() OVER(PARTITION BY T2.craftsman_id ORDER BY count_product_by_master DESC) AS rank_count_product_by_master 
                        FROM ( 
                            SELECT-- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
                                T1.customer_id AS customer_id,
                                T1.customer_name AS customer_name,
                                T1.customer_address AS customer_address,
                                T1.customer_birthday AS customer_birthday,
                                T1.customer_email AS customer_email,
                                SUM(T1.product_price) AS customer_money,
                                SUM(T1.product_price) * 0.1 AS platform_money,
                                COUNT(order_id) AS count_order,
                                AVG(T1.product_price) AS avg_price_order,
                                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,                              
                                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
                                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
                                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
                                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                                T1.report_period AS report_period
                               FROM (
                               		SELECT dcs.customer_id AS customer_id,
                               			   dcs.customer_name AS customer_name,
                               			   dcs.customer_addresss as customer_address,
                               			   dcs.customer_birthday AS customer_birthday,
                               			   dcs.customer_email AS customer_email,
                               			   dp.product_id as product_id,
                               			   dp.product_price as product_price,
                               			   dp.product_type as product_type,
                               			   fo.order_id as order_id,
                               			   fo.order_completion_date - fo.order_created_date AS diff_order_date,
                               			   fo.order_status AS order_status, 
                               			   TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
                               		FROM dwh.f_order fo
                               			INNER JOIN dwh.d_customer dcs 
                               				ON fo.customer_id = dcs.customer_id
                               			INNER JOIN dwh.d_craftsman dc
                               				ON fo.craftsman_id = dc.craftsman_id 
                               			INNER JOIN dwh.d_product dp 
                               				ON fo.product_id = dp.product_id
                               			INNER JOIN dwh_update_delta ud 
                               				ON fo.craftsman_id = ud.craftsman_id
                               		) AS T1 
                               	group BY T1.customer_id,
                               			 T1.customer_name,
                               			 T1.customer_address,
                               			 T1.customer_birthday,
                               			 T1.customer_email,
                               			 T1.report_period
                            ) AS T2 
                                INNER JOIN (
                                    SELECT --Определяем самую популярную категорию продукта у заказчика
                                            dd.customer_id AS customer_id_for_product_type, 
                                            dd.product_type, 
                                            COUNT(dd.product_id) AS count_product_by_type
                                            FROM dwh_delta AS dd
                                                GROUP BY dd.customer_id, 
                                                		dd.product_type
                                                    ORDER BY count_product_by_type DESC
                               ) AS T3 
                              	ON T2.customer_id = T3.customer_id_for_product_type
                              INNER JOIN (
                                    SELECT --Определяем самого популярного мастера у заказчика
                                            dd.customer_id AS customer_id_for_top_master, 
                                            dd.craftsman_id, 
                                            COUNT(dd.product_id) AS count_product_by_master
                                            FROM dwh_delta AS dd
                                                GROUP BY dd.customer_id, 
                                                		 dd.craftsman_id
                                                    ORDER BY count_product_by_master DESC
                                ) AS T4 
                              ON T2.customer_id = T4.customer_id_for_top_master
                ) AS T5 
               WHERE T5.rank_count_product_by_type = 1 
               	AND T5.rank_count_product_by_master = 1
               ORDER BY report_period -- условие помогает оставить в выборке первую по популярности категорию товаров
),

insert_delta AS (-- выполняем insert новых расчитанных данных для витрины 
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday, 
        customer_email, 
        customer_money, 
        platform_money, 
        count_order, 
        avg_price_order, 
        median_time_order_completed,
        top_product_category, 
        top_master_id,
        count_order_created, 
        count_order_in_progress, 
        count_order_delivery, 
        count_order_done, 
        count_order_not_done, 
        report_period
    ) SELECT 
            customer_id,
            customer_name,
        	customer_address,
        	customer_birthday, 
        	customer_email, 
        	customer_money, 
        	platform_money, 
        	count_order, 
        	avg_price_order, 
        	median_time_order_completed,
        	top_product_category, 
        	top_master_id,
        	count_order_created, 
        	count_order_in_progress, 
        	count_order_delivery, 
        	count_order_done, 
        	count_order_not_done, 
        	report_period
       FROM dwh_delta_insert_result
),

update_delta AS (-- выполняем обновление показателей в отчёте по уже существующим мастерам
    UPDATE dwh.customer_report_datamart SET
        customer_name = updates.customer_name, 
        customer_address = updates.customer_address, 
        customer_birthday = updates.customer_birthday, 
        customer_email = updates.customer_email, 
        customer_money = updates.customer_money, 
        platform_money = updates.platform_money, 
        count_order = updates.count_order, 
        avg_price_order = updates.avg_price_order, 
        median_time_order_completed = updates.median_time_order_completed, 
        top_product_category = updates.top_product_category, 
        top_master_id = updates.top_master_id,
        count_order_created = updates.count_order_created, 
        count_order_in_progress = updates.count_order_in_progress, 
        count_order_delivery = updates.count_order_delivery, 
        count_order_done = updates.count_order_done,
        count_order_not_done = updates.count_order_not_done, 
        report_period = updates.report_period
    FROM (
        SELECT 
            customer_id,
            customer_name,
        	customer_address,
        	customer_birthday, 
        	customer_email, 
        	customer_money, 
        	platform_money, 
        	count_order, 
        	avg_price_order, 
        	median_time_order_completed,
        	top_product_category, 
        	top_master_id,
        	count_order_created, 
        	count_order_in_progress, 
        	count_order_delivery, 
        	count_order_done, 
        	count_order_not_done, 
        	report_period 
            FROM dwh_delta_update_result) AS updates
    WHERE dwh.customer_report_datamart.customer_id = updates.customer_id
),

insert_load_date AS ( -- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
        FROM dwh_delta
)

SELECT 'increment datamart'; -- инициализируем запрос CTE 


