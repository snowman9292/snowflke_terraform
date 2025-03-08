
{{ config(materialized='incremental') }}
with customer as
(
select CUSTOMER_ID ,
SALUTATION||' '||FIRST_NAME||' '||LAST_NAME AS CUSTOMER_NAME,
to_date(BIRTH_MONTH||'/'||BIRTH_DAY||'/'||BIRTH_YEAR,'mm/dd/yyyy' )AS CUSTOMER_DOB,
BIRTH_COUNTRY,
EMAIL_ADDRESS,
{{get_current_timestamp()}} AS CREATED_DT,  
{{get_updt_timestamp() }} AS UPDATED_DT
 from 
    {{source ('CUSTOMER_RAW_DATA','CUSTOMER_RAW')}}

)

select * from customer