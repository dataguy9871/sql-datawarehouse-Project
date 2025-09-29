EXECUTE silver.load_silver
Create or Alter Procedure silver.load_silver AS
BEGIN
DECLARE @start_time datetime, @end_time datetime, @start_batch_time datetime, @end_batch_time datetime

begin try
set  @start_batch_time =  getdate()
PRINT '=========================================================';
PRINT 'Loading Silver Layer';
PRINT '=========================================================';


PRINT'----------------------------------';
PRINT'Loading CRM Tables';
PRINT'----------------------------------';

Set @start_time = getdate();
Print'>> Truncating Table :silver.crm_cust_info ';
Truncate Table silver.crm_cust_info;
Print'>> Inserting Data Into:silver.crm_cust_info ';
Insert into silver.crm_cust_info
(cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname , -- To remove unwanted space from first_name
trim(cst_lastname) as cst_lastname, -- To remove unwanted space from last_name
case when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
     when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
	 else 'n/a' end as cst_marital_status,
case when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
     when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
	 else 'n/a' end as cst_gndr,
cst_create_date
from
(select *,
row_number()over(partition by cst_id order by cst_create_date ) as flag_raise --To remove null and duplicate values from primary key
from [bronze].[crm_cust_info]
where cst_id is not null) t
where flag_raise =1
Set @end_time = getdate();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '---------------------------';


Set @start_time = getdate();
Print'>> Truncating Table :[silver].[crm_prd_info] ';
Truncate Table [silver].[crm_prd_info];
Print'>> Inserting Data Into:[silver].[crm_prd_info] ';
insert into [silver].[crm_prd_info](
prd_id,prd_key,cat_id,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
)
SELECT
    prd_id,
    REPLACE(LEFT(prd_key, 5), '-', '_') AS prd_key,
    SUBSTRING(prd_key, 7, LEN(prd_key) - 6) AS cat_id,   -- avoid alias clash
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(LTRIM(RTRIM(prd_line)))
         WHEN 'R' THEN 'Roads'
         WHEN 'M' THEN 'Mountain'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Transport'
         ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS date) AS prd_start_dt,
    DATEADD(day, -1,
        LEAD(CAST(prd_start_dt AS date))
        OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
    ) AS prd_end_dt
FROM [bronze].[crm_prd_info];
Set @end_time = getdate();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '---------------------------';


Set @start_time = getdate();
Print'>> Truncating Table :silver.crm__sales_details ';
Truncate Table silver.crm__sales_details;
Print'>> Inserting Data Into:silver.crm__sales_details';
insert into silver.crm__sales_details (sls_ord_num
      ,sls_prd_key
      ,sls_cust_id
      ,sls_order_dt
      ,sls_ship_dt
      ,sls_due_dt
      ,sls_sales
      ,sls_quantity
      ,sls_price)
SELECT sls_ord_num
      ,sls_prd_key
      ,sls_cust_id,
	  case when sls_order_dt < 0 or len(sls_order_dt) != 8  then Null
	   else cast(cast(sls_order_dt as varchar) as date) end as sls_order_dt
,case when sls_ship_dt < 0 or len(sls_ship_dt) != 8  then Null
	   else cast(cast(sls_ship_dt as varchar) as date) end as sls_ship_dt,
	  case when sls_due_dt < 0 or len(sls_due_dt) != 8  then Null
	   else cast(cast(sls_due_dt as varchar) as date) end as sls_due_dt
      ,
	  case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs( sls_price ) then sls_quantity * abs(sls_price) 
	  else sls_sales end as sls_sales,
      sls_quantity
      ,
	  case when sls_price <= 0 or sls_price is null then sls_sales / nullif(sls_quantity,0)
	  else sls_price end as sls_price
  FROM [bronze].[crm__sales_details]
  Set @end_time = getdate();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '---------------------------';

Print '-------------------------------------------';
Print 'Loading ERM Tables';
Print '-------------------------------------------';

Set @start_time = getdate();
  Print'>> Truncating Table :[silver].[erp_cust_az12] ';
Truncate Table [silver].[erp_cust_az12];
Print'>> Inserting Data Into:[silver].[erp_cust_az12] ';
  Insert into [silver].[erp_cust_az12] ( cid, bdate, gen)
  select 
  Case when CID LIKE 'NAS%' THEN substring(CID, 4, LEN(CID)) 
  else CID end as cid, 
  case when BDATE > getdate() then Null
  else BDATE end as bdate,
  case when Upper(trim(GEN)) IN ('F', 'FEMALE') THEN 'Female'
        when Upper(trim(GEN)) IN ('M', 'MALE') THEN 'Male'
		else 'n/a' end as gen
  from [bronze].[erp_cust_az12]
  Set @end_time = getdate();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '---------------------------';

  Set @start_time = getdate();
   Print'>> Truncating Table :[silver].[erp_loc_a101] ';
Truncate Table [silver].[erp_loc_a101];
Print'>> Inserting Data Into:[silver].[erp_loc_a101]';
   Insert into [silver].[erp_loc_a101] (cid,cntry)
   Select replace(CID, '-','') as CID,
   case when upper(trim(cntry)) in ('DE', 'GERMANY') THEN 'Germany'
        when upper(trim(cntry)) in ('USA', 'US', 'UNITED STATES') THEN 'United States'
		when (cntry is NULL or cntry ='') THEN 'n/a'
		else trim(cntry) end as cntry
  from [bronze].[erp_loc_a101]
  Set @end_time = getdate();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '---------------------------';

  Set @start_time = getdate();
 Print'>> Truncating Table :[silver].[erp_px_cat_g1v2]';
Truncate Table [silver].[erp_px_cat_g1v2];
Print'>> Inserting Data Into:[silver].[erp_px_cat_g1v2]';
Insert into [silver].[erp_px_cat_g1v2] (ID, CAT, SUBCAT, MAINTENANCE )
select ID, CAT, SUBCAT, MAINTENANCE from [bronze].[erp_px_cat_g1v2]
Set @end_time = getdate();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '---------------------------';

set @end_batch_time = getdate();
print '>> Complete Load Duration: ' + cast(datediff(second, @start_batch_time, @end_batch_time) as nvarchar) + 'seconds';
print '---------------------------';

END TRY
BEGIN CATCH
PRINT '--------------------------------------';
PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
PRINT 'Error Message' + Error_Message();
PRINT 'Error Message' + cast(Error_Message() as nvarchar);
PRINT 'Error Message' + cast(Error_number() as nvarchar);
PRINT '--------------------------------------';
END CATCH
END



























