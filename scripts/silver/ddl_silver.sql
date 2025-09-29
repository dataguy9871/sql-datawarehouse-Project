if OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
Drop table silver.crm_cust_info;

Create Table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),	
cst_gndr nvarchar(50),	
cst_create_date Date,
dwh_create_date datetime2 default getdate()
)

if OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
Drop table silver.crm_prd_info;
Create table silver.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
cat_id NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date datetime2 default getdate()
)

if OBJECT_ID ('silver.crm__sales_details', 'U') IS NOT NULL
Drop table silver.crm__sales_details;
Create table silver.crm__sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,	
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date datetime2 default getdate()
)

if OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
Drop table silver.erp_cust_az12;
Create table silver.erp_cust_az12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50),
dwh_create_date datetime2 default getdate()
)

if OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
Drop table silver.erp_loc_a101;
Create table silver.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50),
dwh_create_date datetime2 default getdate()
)


if OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
Drop table silver.erp_px_cat_g1v2;
Create table silver.erp_px_cat_g1v2(
ID nvarchar(50),	
CAT	 nvarchar(50),
SUBCAT	nvarchar(50),
MAINTENANCE nvarchar(50),
dwh_create_date datetime2 default getdate()
)
