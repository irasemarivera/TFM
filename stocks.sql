create table stocks(
	stock varchar(5) primary key,
	company_name varchar(100),
	sector varchar(50),
	subindustry varchar(100)
)

insert into stocks(stock, company_name, sector, subindustry) values ('GOOG','Alphabet Inc. (Class C)','Communication Services','Interactive Media & Services');
insert into stocks(stock, company_name, sector, subindustry) values ('AXP','American Express Co','Financials','Consumer Finance');
insert into stocks(stock, company_name, sector, subindustry) values ('BA','Boeing Company','Industrials','Aerospace & Defense');
insert into stocks(stock, company_name, sector, subindustry) values ('KO','Coca-Cola Company','Consumer Staples','Soft Drinks');
insert into stocks(stock, company_name, sector, subindustry) values ('D','Dominion Energy','Utilities','Electric Utilities');
insert into stocks(stock, company_name, sector, subindustry) values ('DD','DuPont de Nemours Inc','Materials','Specialty Chemicals');
insert into stocks(stock, company_name, sector, subindustry) values ('XOM','Exxon Mobil Corp.','Energy','Integrated Oil & Gas');
insert into stocks(stock, company_name, sector, subindustry) values ('IBM','International Business Machines','Information Technology','IT Consulting & Other Services');
insert into stocks(stock, company_name, sector, subindustry) values ('PFE','Pfizer Inc.','Health Care','Pharmaceuticals');
insert into stocks(stock, company_name, sector, subindustry) values ('SPG','Simon Property Group Inc','Real Estate','Retail REITs');
insert into stocks(stock, company_name, sector, subindustry) values ('TSLA','Tesla, Inc.','Consumer Discretionary','Automobile Manufacturers');

create table stocks_history(
	stock varchar(5) not null,
	stock_date datetime not null,
	high_value numeric not null,
	low_value numeric not null,
	open_value numeric not null,
	close_value numeric not null,
	volume numeric not null,
	adj_close numeric not null,
	primary key (stock, stock_date)
)

select s.stock, max(sh.stock_date) 
from stocks s left join stocks_history sh on s.stock = sh.stock
group by s.stock

select count(*) from stocks_history

create table news_sources(
	source_id int not null identity primary key,
	source_name varchar(100)
)

insert into news_sources(source_name) values ('Financial Times');
insert into news_sources(source_name) values ('The Wall Street Journal');
insert into news_sources(source_name) values ('International Business Times');
insert into news_sources(source_name) values ('Reuters');
insert into news_sources(source_name) values ('Business Insider');
insert into news_sources(source_name) values ('The economist');
insert into news_sources(source_name) values ('Bloomberg');
insert into news_sources(source_name) values ('Businessweek');
insert into news_sources(source_name) values ('Forbes');
insert into news_sources(source_name) values ('Fortune');
insert into news_sources(source_name) values ('CNN');
insert into news_sources(source_name) values ('Money CNN');
insert into news_sources(source_name) values ('Yahoo Finance');
insert into news_sources(source_name) values ('Euromoney');
insert into news_sources(source_name) values ('BBC');
insert into news_sources(source_name) values ('The New York Times');
insert into news_sources(source_name) values ('The Washington Post');

select * from news_sources