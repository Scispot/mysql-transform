
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with get_boxes as (
    select 
        boxes._airbyte_ab_id as glue_id,
        json_extract(boxes._airbyte_data,'$.id') as box_id,
        json_extract(boxes._airbyte_data,'$.name') as box_name,
        json_extract(boxes._airbyte_data,'$.webURL') as web_url,
        json_extract(boxes._airbyte_data, '$.barcode') AS barcode,
        json_extract(boxes._airbyte_data, '$.creator.name') AS creator_name,
        json_extract(boxes._airbyte_data, '$.creator.handle') AS creator_handle,
        json_extract(boxes._airbyte_data, '$.schema.name') AS box_schema_name,
        json_extract(boxes._airbyte_data, '$.emptyPositions') AS empty_positions,
        json_extract(boxes._airbyte_data, '$.filledPositions') AS filled_positions,
        json_extract(boxes._airbyte_data, '$.size') AS size,
        json_extract(boxes._airbyte_data, '$.emptyContainers') AS empty_containers,
        json_extract(boxes._airbyte_data, '$.parentStorageId') AS parent_storage_id,
        json_extract(boxes._airbyte_data, '$.createdAt') AS created_at,
        json_extract(boxes._airbyte_data, '$.modifiedAt') AS modified_at,
        boxes._airbyte_emitted_at as emitted_at
         from sandbox._airbyte_raw_get_boxes as boxes
)

select *
from get_boxes

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
