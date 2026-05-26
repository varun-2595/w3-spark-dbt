select
    cast(rate_code_id as integer) as rate_code_id,
    cast(description as varchar(50)) as description
from (
    values
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride')
) as t(rate_code_id, description)
