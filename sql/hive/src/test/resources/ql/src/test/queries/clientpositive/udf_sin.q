DESCRIBE FUNCTION sin;
DESCRIBE FUNCTION EXTENDED sin;

SELECT sin(null)
FROM src LIMIT 1;

SELECT sin(0.98), sin(1.57), sin(-0.5)
FROM src LIMIT 1;
