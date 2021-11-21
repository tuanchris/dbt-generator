with source as (

    select * from {{ source('GOOGLE_ADS', 'ACCOUNTS') }}

),

renamed as (

    select
        canmanageclients as can_manage_clients,
        currencycode as currency_code,
        cast(customerid as int64) as customer_id,
        datetimezone as date_timezone,
        name as name,
        testaccount as test_account,
        timestamp(_sdc_batched_at) as _sdc_batched_at,
        cast(_sdc_customer_id as int64) as _sdc_customer_id,
        timestamp(_sdc_extracted_at) as _sdc_extracted_at,
        timestamp(_sdc_received_at) as _sdc_received_at,
        _sdc_sequence as _sdc_sequence,
        _sdc_table_version as _sdc_table_version

    from source

)

select * from renamed
