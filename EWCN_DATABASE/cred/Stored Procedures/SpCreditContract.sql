-- ************************************************************************************************
-- Original Code created by EWCN 
-- Create Date: 2023-08
-- base on Undraw report and CAD 台账
-- ************************************************************************************************

CREATE PROCEDURE  [cred].[SpCreditContract]
	@DataDateX DATE, @BatchID INT = 0 --0 Default Value
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

-- ************************************************************************************************
-- SET up logging
-- ************************************************************************************************
DECLARE @DBName VARCHAR(200) = DB_NAME();
DECLARE @SPName VARCHAR(200) = 'cred.SpCreditContract';
DECLARE @SpStartTime  DATETIME = GETDATE();
 
 -- ************************************************************************************************
-- Declare variables
-- ************************************************************************************************
	DECLARE @PROCESSDATE DATE =EOMONTH(@DataDateX)
	

-- ************************************************************************************************
-- Drop temp table
-- ************************************************************************************************
	drop table if exists #creditContract
	DROP TABLE IF EXISTS #BorrowerSplit
	DROP TABLE IF EXISTS #GetLoanBal
	DROP TABLE IF EXISTS #GetMainBorrower


-- ************************************************************************************************
-- CREATE temp TABLE
-- ************************************************************************************************
CREATE TABLE #creditContract(
	[branch] [varchar](20) NULL,
	[FacilityAgreementNumber] [nvarchar](100) NOT NULL,
	[itemcode] [varchar](10) NULL,
	[AllBorrowerCif] [nvarchar](100) NULL,
	[AllBorrowerName] [nvarchar](200) NULL,
	[MainApplicantCif] [varchar](20) NULL,
	[ContractCreditCCY] [varchar](20) NULL,
	[TotalContractCredit] [decimal](20, 2) NULL,
	[OnOffBalanceSheet] [varchar](20) NULL,
	[NBFI] [varchar](20) NULL,
	[CommitmentStatus] [varchar](20) NULL,
	[FacilityTerm] [varchar](20) NULL,
	[LoanOustandingAmount] [decimal](20, 2) NULL,
	[ContingentOutstandingAmount] [decimal](20, 2) NULL,
	[TotalAmount] [decimal](20, 2) NULL,
	UnusedFacilitiy [decimal](20, 2) NULL, -- 匹配合同币种的金额
	[UnusedFacilitiyAmountCNY] [decimal](20, 2) NULL,
	[UnusedFacilitiyAmountUSD] [decimal](20, 2) NULL,
	[LoanAccount] [nvarchar](200) NULL,
	[VALDATE] [date] NULL,
	[SIGDATE] [date] NULL,
	[DUEDATE] [date] NULL,
	[AVLDATE] [date] NULL,
	[BFC] [varchar](20) NULL,
	[RM] [varchar](100) NULL,
	[PLOANIND] [varchar](20) NULL,
	[STATUS] [varchar](20) NULL,
	[REC_APP] [varchar](20) NULL,
	[REC_INP] [varchar](20) NULL,
	[CCRCODE] [varchar](100) NULL,
	[processdate] [date] NOT NULL,
PRIMARY KEY  ([FacilityAgreementNumber] ,[processdate] )
)

insert into #creditContract
select A.branch
	,A.FacilityAgreementNumber
	,  case when A.nbfi='Y' then '70903'
			when A.nbfi='N' and A.onoffbalancesheet='N'  then '70909'
			when A.nbfi='N' and A.onoffbalancesheet='Y'  and A.commitmentstatus='Y' then '70902'
			when A.nbfi='N' and A.onoffbalancesheet='Y'  and A.commitmentstatus='N' then '70901'
			else null
			end as itemcode
	,ltrim(rtrim(A.CustomerNumber)) AS AllBorrowerCif
	,A.Borrower AS AllBorrowerName
	,MainApplicantCif=null
	, case when A.ContractCreditCCY	='RMB' then 'CNY' else A.ContractCreditCCY end
	,A.TotalContractCredit	
	,A.OnOffBalanceSheet
	,A.NBFI
	,A.CommitmentStatus
	,A.FacilityTerm  -- S: <=1 Year ; T >1 Year
	,A.LoanOustandingAmount
	,A.ContingentOutstandingAmount
	,A.TotalAmount
	,UnusedFacilitiy=0
	,isnull(A.UnusedFacilitiyAmountCNY,0)
	,isnull(A.UnusedFacilitiyAmountUSD,0)
	,ltrim(rtrim(A.LoanAccount)) LoanAccount
	,CAST(B.VALDATE AS DATE) AS VALDATE
	,CAST (B.SIGDATE AS DATE) AS SIGDATE
	,CAST (B.DUEDATE AS DATE) AS DUEDATE
	,CAST (B.AVLDATE AS DATE) AS AVLDATE
	,B.BFC --
	,B.RM
	,B.PLOANIND
	,B.[STATUS] 
	,B.REC_APP
	,B.REC_INP
	,B.CCRCODE
	,A.processdate
from dbo.XUndrawReportRevised a
	LEFT JOIN ODS_China.[dvloans].[contract] B
			ON a.FacilityAgreementNumber=B.CODE
			AND B.PROCESS_DT=@PROCESSDATE
			AND B.REC_STATUS='A'  
where A.processdate=@PROCESSDATE

-- ************************************************************************************************
-- Updates
-- ************************************************************************************************

update A 
set UnusedFacilitiy= case when A.[ContractCreditCCY]='CNY' then A.UnusedFacilitiyAmountCNY
							else  A.UnusedFacilitiyAmountCNY/R.FXRate
							end
from #creditContract A
	LEFT JOIN [dbo].[vwFXRatetoCNY] R
		ON A.[ContractCreditCCY] = R.FromCCY
		AND R.ProcessDate = @ProcessDate
where A.UnusedFacilitiyAmountCNY >0

-- update 主要申请人、
--1. 没有分隔符的
update #creditContract
set MainApplicantCif=AllBorrowerCif
WHERE  CHARINDEX('/',AllBorrowerCif)=0

--2. 拆分带有分隔符的
select A.FacilityAgreementNumber
		,BorrowerPosition=row_number()over(partition by A.FacilityAgreementNumber order by CHARINDEX('/'+  b.value+ '/',  '/' + A.AllBorrowerCif + '/' ))
		,Borrower= b.value
into #BorrowerSplit
from #creditContract  A
	CROSS APPLY String_Split(AllBorrowerCif, '/') AS B
where  MainApplicantCif is null

select A.FacilityAgreementNumber
	,a.BorrowerPosition
	,a.Borrower
	,sum(isnull(b.LoanBalance,0)) as LoanBalTotal
into #GetLoanBal
from #BorrowerSplit A
	left join ln.vwLoan B
		on B.ProcessDate=@PROCESSDATE
		and LoanBalance>0
		and substring(refno,1,2) in ('CN','SN') 
		and A.Borrower=b.CustomerID
		and A.FacilityAgreementNumber=b.mainContractCode
group by A.FacilityAgreementNumber,a.BorrowerPosition,a.Borrower
order by A.FacilityAgreementNumber,a.BorrowerPosition,a.Borrower

-- 根据有业务的，并且填写靠前的，获取作为主要借款人
select b.FacilityAgreementNumber
	,b.Borrower
into #GetMainBorrower
from (
	select  A.FacilityAgreementNumber
		,a.BorrowerPosition
		,a.Borrower
		, ROW_NUMBER()over(partition by A.FacilityAgreementNumber order by a.BorrowerPosition) as  nm
	from #GetLoanBal A 
	where LoanBalTotal>0
	) B
where b.nm=1
order by b.FacilityAgreementNumber

update A
set A.MainApplicantCif=b.Borrower
from #creditContract A
	inner join #GetMainBorrower B
		on A.FacilityAgreementNumber=b.FacilityAgreementNumber

-- 拆分贷款账号

select A.FacilityAgreementNumber
		,AccountPosition=row_number()over(partition by A.FacilityAgreementNumber order by CHARINDEX('/'+  b.value+ '/',  '/' + A.AllBorrowerCif + '/' ))
		,loanAccount= b.value
INTO #AccountSplit
from #creditContract  A
	CROSS APPLY String_Split(loanAccount, '/') AS B
where  MainApplicantCif is null

select B.FacilityAgreementNumber,B.loanAccount,B.AccountPosition
into #GetBorrowerActiveAcc
from (
	select A.FacilityAgreementNumber,a.loanAccount,a.AccountPosition
		,ROW_NUMBER()over(partition by FacilityAgreementNumber order by AccountPosition) as nm
	from #AccountSplit A
		inner join ODS_China.dbo.DEPACDTL B
			on B.PROCESS_DT=@PROCESSDATE
			and recsts not in ('C','R')
			and A.loanAccount=b.ACCNO
		)B
where b.nm=1

-- 如果Allborrow的cif 都没业务，就取active的贷款账号位置的 borrower
update A
set A.MainApplicantCif=C.Borrower
from #creditContract A
	inner join #GetBorrowerActiveAcc B
		on A.FacilityAgreementNumber=b.FacilityAgreementNumber
	inner join #BorrowerSplit C
		on b.FacilityAgreementNumber=c.FacilityAgreementNumber
		and b.AccountPosition=c.BorrowerPosition
where A.MainApplicantCif is null

-- 如果没业务，也没active的存款， 就取第一位置的CIF
update A
set A.MainApplicantCif=C.Borrower
from #creditContract A
	inner join #BorrowerSplit C
		on A.FacilityAgreementNumber=c.FacilityAgreementNumber
		and c.BorrowerPosition=1
where A.MainApplicantCif is null

-- ************************************************************************************************
-- Delete data from real table
-- ************************************************************************************************
delete from cred.creditContract where processdate=@PROCESSDATE

-- ************************************************************************************************
-- Insert data into real table
-- ************************************************************************************************
insert into cred.creditContract
select  getdate()
	,*
from #creditContract

-- ************************************************************************************************
-- Execute logging
-- ************************************************************************************************
	EXEC MGT.dbo.SpSprocLog  @DBName, @SPName, @SpStartTime, @BatchID

-- ************************************************************************************************
-- Done
-- ************************************************************************************************
end