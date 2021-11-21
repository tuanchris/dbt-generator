with source as (

    select * from {{ source('GOOGLE_ADS', 'ACCOUNTS') }}

),

renamed as (

    select
        canmanageclients as can_manage_clients,
        currencycode as currency_code,
        customerid::integer as customer_id,
        datetimezone as date_timezone,
        name as name,
        testaccount as test_account,
        _sdc_batched_at::timestamp as _sdc_batched_at,
        _sdc_customer_id::integer as _sdc_customer_id,
        _sdc_extracted_at::timestamp as _sdc_extracted_at,
        _sdc_received_at::timestamp as _sdc_received_at,
        _sdc_sequence as _sdc_sequence,
        _sdc_table_version as _sdc_table_version

    from source

)

select * from renamed
