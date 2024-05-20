/* Таблица контрактов */
CREATE TABLE public.acnt_contract2 (
  id serial4 NOT NULL,
  client_id serial4 NOT NULL,
  date_open timestamp NULL,
  contract_number varchar NULL,
  account varchar NULL,
  CONSTRAINT acnt_contract2_pk PRIMARY KEY (id)
);

CREATE SEQUENCE public.acnt_contract2_id_seq
  INCREMENT BY 1
  MINVALUE 1
  MAXVALUE 2147483647
  START 1
  CACHE 1
  NO CYCLE;

/* Таблица клиентов */
CREATE TABLE public.clients (
  id serial4 NOT NULL,
  gender varchar NULL,
  "e-mail" varchar NULL,
  address varchar NULL,
  city varchar NULL,
  phone varchar NULL,
  short_name varchar(100) NULL,
  birthday date NULL,
  CONSTRAINT clients_pk PRIMARY KEY (id)
);

CREATE SEQUENCE public.clients_id_seq
  INCREMENT BY 1
  MINVALUE 1
  MAXVALUE 2147483647
  START 1
  CACHE 1
  NO CYCLE;

/* Тестовый скрипт */
select
 c.short_name client,
 ac.contract_number pan,
 ac.account ,
 c.city || ', ' || c.address address,
 '+7'||replace(substr(c.phone,3), '-','') phone ,
 c."e-mail",
 trunc(1.14 * cos(c.id) * sin(ac.id * 0.2) * cos(ac.id / 1.2)*1000000) trans_amount
 from acnt_contract2 ac,
 clients  c
 where
 ac.client_id  = c.id;

/* Распараллеленный тестовый скрипт */
select
 c.short_name client,
 ac.contract_number pan,
 ac.account ,
 c.city || ', ' || c.address address,
 '+7'||replace(substr(c.phone,3), '-','') phone ,
 c."e-mail",
 trunc(1.14 * cos(c.id) * sin(ac.id * 0.2) * cos(ac.id / 1.2)*1000000) trans_amount
 from acnt_contract2 ac,
 clients  c
 where
 ac.client_id  = c.id
 /* Первый параметр - общее количество потоков выполнения
    Второй параметр - номер потока выполнения, начиная с 0
 */
 and mod(ac.id, ?) = ?;