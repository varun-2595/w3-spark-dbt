select
    cast(payment_type_id as integer) as payment_type_id,
    cast(description as varchar(50)) as description
from (
    values
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
) as t(payment_type_id, description)
