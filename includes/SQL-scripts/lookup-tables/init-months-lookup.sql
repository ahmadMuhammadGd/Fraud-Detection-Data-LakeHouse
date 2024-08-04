CREATE TABLE IF NOT EXISTS nessie.months_lookup 
(id INT, month_name STRING)
USING iceberg;

INSERT INTO
    nessie.months_lookup (id, month_name)
VALUES
    (1, 'JAN'),
    (2, 'FEB'),
    (3, 'MAR'),
    (4, 'APR'),
    (5, 'MAY'),
    (6, 'JUN'),
    (7, 'JUL'),
    (8, 'AUG'),
    (9, 'SEP'),
    (10, 'OCT'),
    (11, 'NOV'),
    (12, 'DEC');