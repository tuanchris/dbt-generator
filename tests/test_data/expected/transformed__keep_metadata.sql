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
        testaccount,
        _sdc_batched_at,
        _sdc_customer_id,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_sequence as _sdc_seq,
        _sdc_table_version

    from source

)

select * from renamed
