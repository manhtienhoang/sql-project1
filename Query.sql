--retention rate
with account_first_transactions as(
	select AccountID, month(min(TransactionDateTime)) as first_month 
	from Transactions
	group by AccountID
),
new_account_by_month as(
	select first_month, count(AccountID) as new_account
	from account_first_transactions
	group by first_month
), 
account_retention_month as(
	select AccountID, month(TransactionDateTime) as retention_month
	from Transactions
	group by AccountID, month(TransactionDateTime)
),
retained_accounts_by_month as(
	select b.first_month, a.retention_month, count(a.AccountID) as retained_accounts
	from account_retention_month a
	left join account_first_transactions b
	on a.AccountID = b.AccountID
	group by b.first_month, a.retention_month
)
select r.first_month, r.retention_month, n.new_account, r.retained_accounts, 
r.retained_accounts / n.new_account as retention_rate
from retained_accounts_by_month r
left join new_account_by_month n
on r.first_month = n.first_month
order by 1,2

--RFM Segment
with rfm as(
	select AccountID, cast(max(TransactionDateTime) as date) as last_active_day, 
	DATEDIFF(DAY, max(TransactionDateTime), getdate()) as recency,
	count(distinct TransactionID) as frequency,
	sum(AmountOfTransaction) as monetary
	from Transactions
	group by AccountID
),
rfm_percent_rank as(
	select *,
		PERCENT_RANK() over(order by recency) as recency_percent_rank,
		PERCENT_RANK() over(order by frequency) as frequency_percent_rank,
		PERCENT_RANK() over(order by monetary) as monetary_percent_rank
	from rfm
),
rfm_rank as(
	select *,
		case when recency_percent_rank > 0.75 then 4
		when recency_percent_rank > 0.5 then 3
		when recency_percent_rank > 0.25 then 2
		else 1
		end as recency_rank,
		case when frequency_percent_rank > 0.75 then 4
		when frequency_percent_rank > 0.5 then 3
		when frequency_percent_rank > 0.25 then 2
		else 1 end
		as frequency_rank,
		case when monetary_percent_rank > 0.75 then 4
		when monetary_percent_rank > 0.5 then 3
		when monetary_percent_rank > 0.25 then 2
		else 1 end
		as monetary_rank
	from rfm_percent_rank
),
rfm_rank_concat as(
	select *, concat(recency_rank, frequency_rank, monetary_rank) as rfm_rank
	from rfm_rank
)
select *,
case WHEN rfm_rank  =  111 THEN 'Best Customers'
        WHEN rfm_rank LIKE '[3-4][3-4][1-4]' THEN 'Lost Bad Customer'
        WHEN rfm_rank LIKE '[3-4]2[1-4]' THEN 'Lost Customers'
        WHEN rfm_rank LIKE  '21[1-4]' THEN 'Almost Lost'
        WHEN rfm_rank LIKE  '11[2-4]' THEN 'Loyal Customers'
        WHEN rfm_rank LIKE  '[1-2][1-3]1' THEN 'Big Spenders'
        WHEN rfm_rank LIKE  '[1-2]4[1-4]' THEN 'New Customers'
        WHEN rfm_rank LIKE  '[3-4]1[1-4]' THEN 'Hibernating'
        WHEN rfm_rank LIKE  '[1-2][2-3][2-4]' THEN 'Potential Loyalists'
	else 'Unknown'
end 
as rfm_segment
from rfm_rank_concat

--So lan giao dich cua tung service
select t.ServiceID, count(t.TransactionID) as NoTransactions
from Transactions t 
join Service s
on t.ServiceID = s.ServiceID
group by t.ServiceID

--service nao dc sd nhieu nhat
with most_service_by_notransaction as(
	select t.ServiceID, count(t.TransactionID) as NoTransactions,
	rank() over(order by count(t.TransactionID) desc) as rnk
	from Transactions t 
	join Service s
	on t.ServiceID = s.ServiceID
	group by t.ServiceID
)
select *
from most_service_by_notransaction
where rnk = 1

--So tien da giao dich trong tung thang cua tung account
select AccountID, year(TransactionDateTime) as year, month(TransactionDateTime) as month, sum(AmountOfTransaction) as TotalAmount
from Transactions
group by AccountID, year(TransactionDateTime), month(TransactionDateTime)

--%So tien da giao dich trong tung thang cua tung account theo tung dich vu tren tong so tien trong thang do
with amount_by_service_and_month as(
	select AccountID, year(TransactionDateTime) as year, month(TransactionDateTime) as month, serviceID, sum(AmountOfTransaction) as Amount
	from Transactions
	group by AccountID, year(TransactionDateTime), month(TransactionDateTime), serviceID
),
total_amount_by_month as(
	select AccountID, year(TransactionDateTime) as year, month(TransactionDateTime) as month, sum(AmountOfTransaction) as TotalAmount
	from Transactions
	group by AccountID,year(TransactionDateTime), month(TransactionDateTime)
)
select a.AccountID, a.year, a.month, a.Amount as ServiceAmount, b.TotalAmount, (a.Amount / b.TotalAmount)*100 as PercentOfTotalAmount
from amount_by_service_and_month a
join total_amount_by_month b
on a.AccountID = b.AccountID and a.year = b.year and a.month = b.month

--Tong so account dang hoat dong
select count(*) as NoAccounts
from Account

--Ngan hang nao duoc lien ket nhieu nhat
with most_bank_linked as(
	select BankID, count(AccountID) as NoLink,
	rank() over(order by count(AccountID) desc) as rnk
	from BankAccount
	group by BankID
)
select *
from most_bank_linked 
where rnk = 1

--%Tang truong so tien giao dich cua thang nay so voi thang truoc theo tung khach hang
select AccountID
	, year(TransactionDateTime) as year
	, month(TransactionDateTime) as month
	, sum(AmountOfTransaction) as TotalAmount
	, lead(sum(AmountOfTransaction), 1, null) over(partition by AccountID order by month(TransactionDateTime)) as TotalAmountNM
	, (-sum(AmountOfTransaction) +  lead(sum(AmountOfTransaction), 1, null) over(partition by AccountID order by month(TransactionDateTime))) / sum(AmountOfTransaction)*100 as PercentGrowth
from Transactions
group by AccountID, year(TransactionDateTime), month(TransactionDateTime)

--Tong gia tri giao dich (GTV - Gross Transaction Value)
select sum(AmountOfTransaction) as GTV
from Transactions

--Tong gia tri giao dich (GTV) theo nam
select year(TransactionDateTime) as year, sum(AmountOfTransaction) as GTV
from Transactions
group by year(TransactionDateTime)

--%Tang truong GTV 
select year(TransactionDateTime) as year
	, sum(AmountOfTransaction) as GTV
	, lag(sum(AmountOfTransaction), 1, null) over (order by year(TransactionDateTime)) as GTVLY
	, (sum(AmountOfTransaction) - lag(sum(AmountOfTransaction), 1, null) over (order by year(TransactionDateTime)))/lag(sum(AmountOfTransaction), 1, null) over (order by year(TransactionDateTime)) as PercentOfGrowth
from Transactions
group by year(TransactionDateTime)

--Tong so user
select count(*) as NoUsers 
from Account

--%Tang truong user qua cac thang
select year(CreateDate) as year
	, month(CreateDate) as month
	, count(*) as NoUsers 
	, lag(count(*), 1, null) over(partition by year(CreateDate) order by month(CreateDate)) as NoUsersLY
	, (count(*) - lag(count(*), 1, null) over(partition by year(CreateDate) order by month(CreateDate)))/(lag(count(*), 1, null) over(partition by year(CreateDate) order by month(CreateDate))) as PercentOfGrowth
from Account
group by year(CreateDate), month(CreateDate)