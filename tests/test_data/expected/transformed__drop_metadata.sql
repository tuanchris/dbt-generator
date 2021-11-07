with source as (

    select * from {{ source('GOOGLE_ADS', 'ACCOUNTS') }}

),

renamed as (

    select
        canmanageclients,
        currencycode,
        CAST(customerid as varchar) as customer_id,
        date_trunc('day',datetimezone) as date_time_zone,
        CAST(NAME as varchar) as col_name,
        testaccount

    from source

)

select * from renamed
