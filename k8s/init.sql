create schema if not exists incrementum;

-- Set the search path to use the incrementum schema by default
set search_path to incrementum, public;

create table incrementum.account (
    id int primary key generated always as identity,
    name varchar(20) not null,
    phone_number varchar(15) not null unique,
    email varchar(50) not null unique,
    password_hash varchar(255) not null,
    api_key varchar(64) not null unique,
    keycloak_id varchar(255) unique null
);

create table incrementum.stock (
    symbol varchar(10) primary key,
    company_name varchar(100) not null,
    updated_at timestamp not null default current_timestamp,
    description TEXT,
    market_cap BIGINT,
    primary_exchange VARCHAR(100),
    type VARCHAR(50),
    currency_name VARCHAR(50),
    cik VARCHAR(50),
    composite_figi VARCHAR(50),
    share_class_figi VARCHAR(50),
    outstanding_shares BIGINT,
    eps NUMERIC(20,6),
    homepage_url VARCHAR(255),
    total_employees INTEGER,
    list_date DATE,
    locale VARCHAR(20),
    sic_code VARCHAR(20),
    sic_description VARCHAR(255)
);

create table incrementum.stock_history (
    stock_symbol varchar(20) not null references incrementum.stock(symbol),
    day_and_time timestamp not null,
    open_price integer not null,
    close_price integer not null,
    high integer not null,
    low integer not null,
    volume integer not null,
    is_hourly boolean default true
);
    
create table incrementum.screener (
    id int primary key generated always as identity,
    screener_name varchar(20) not null,
    description varchar(300)
);

create table incrementum.custom_screener (
    id int primary key generated always as identity,
    account_id int not null references incrementum.account(id),
    screener_name varchar(100) not null,
    created_at timestamp not null default current_timestamp,
    filters json not null
);

create table incrementum.custom_collection (
    id int primary key generated always as identity,
    account_id int not null references incrementum.account(id),
    collection_name varchar(20) not null,
    c_desc varchar(300),
    date_created date not null
);

create table incrementum.custom_collection_stock (
    collection_id int not null references incrementum.custom_collection(id),
    stock_symbol varchar(10) not null references incrementum.stock(symbol)
);

alter table incrementum.custom_collection_stock
    add constraint custom_collection_stock_collection_id_stock_symbol_key unique (collection_id, stock_symbol);

ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS market_cap BIGINT;
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS primary_exchange VARCHAR(100);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS type VARCHAR(50);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS currency_name VARCHAR(50);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS cik VARCHAR(50);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS composite_figi VARCHAR(50);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS share_class_figi VARCHAR(50);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS outstanding_shares BIGINT;
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS eps NUMERIC(20,6);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS homepage_url VARCHAR(255);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS total_employees INTEGER;
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS list_date DATE;
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS locale VARCHAR(20);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS sic_code VARCHAR(20);
ALTER TABLE IF EXISTS incrementum.stock ADD COLUMN IF NOT EXISTS sic_description VARCHAR(255);
