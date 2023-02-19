DROP TABLE IF EXISTS json_order;

CREATE TABLE json_order
(
    order_uid varchar primary key,
    full_order json
);