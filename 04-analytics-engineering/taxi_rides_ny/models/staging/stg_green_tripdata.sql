with

source as (
    select * from {{ source('staging', 'green_tripdata') }}
),

renamed as (
    select 
        *
    from source
)

select * from renamed