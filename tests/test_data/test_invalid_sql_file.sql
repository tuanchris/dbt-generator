with source as (

    select * from {{ source('GOOGLE_ADS', 'ACCOUNTS') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed
