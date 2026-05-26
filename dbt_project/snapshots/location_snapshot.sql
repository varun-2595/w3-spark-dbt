{% snapshot location_snapshot %}

{{
    config(
      target_database='warehouse_db',
      target_schema='snapshots',
      unique_key='location_id',
      strategy='check',
      check_cols=['borough', 'zone_name', 'service_zone'],
    )
}}

select * from {{ ref('dim_location') }}

{% endsnapshot %}
