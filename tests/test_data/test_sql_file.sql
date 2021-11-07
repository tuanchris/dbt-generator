with source as (

    select * from {{ source('GOOGLE_ADS', 'ACCOUNTS') }}

),

renamed as (

    select
        canmanageclients,
        currencycode,
        customerid,
        datetimezone,
        name,
        testaccount,
        _sdc_batched_at,
        _sdc_customer_id,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_sequence,
        _sdc_table_version

    from source

)

select * from renamed
