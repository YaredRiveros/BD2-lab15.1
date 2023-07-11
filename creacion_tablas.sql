-- P1

CREATE TABLE colegio (
    codigo VARCHAR(10),
    nombre VARCHAR(100),
    direccion VARCHAR(200),
    ciudad VARCHAR(100),
    nivel_acad VARCHAR(50),
    total_alumnos INTEGER,
	PRIMARY KEY(codigo, total_alumnos)
) PARTITION BY RANGE (total_alumnos);


-- Partición para TotalAlumnos < 600
CREATE TABLE colegio_p600 PARTITION OF colegio
    FOR VALUES FROM (MINVALUE) TO (600);

-- Partición para 600 <= TotalAlumnos <= 1300
CREATE TABLE colegio_p600_1300 PARTITION OF colegio
    FOR VALUES FROM (600) TO (1300);

-- Partición para TotalAlumnos > 1300
CREATE TABLE colegio_p1300 PARTITION OF colegio
    FOR VALUES FROM (1300) TO (MAXVALUE);



SELECT * FROM colegio_p600;
SELECT * FROM colegio_p600_1300;
SELECT * FROM colegio_p1300;

--- consulta a
SELECT *
FROM (
  (SELECT *
  FROM colegio_p600
  ORDER BY total_Alumnos)
  UNION ALL
  (SELECT *
  FROM colegio_p600_1300
  ORDER BY total_Alumnos)
  UNION ALL
  (SELECT *
  FROM colegio_p1300
  ORDER BY total_Alumnos)
) AS subquery;


--- consulta b

--- Partición intermedia 1: A-L
CREATE TABLE colegio_frag_intermedia1 AS
(
(SELECT *
FROM colegio_p600
WHERE ciudad >= 'A' AND ciudad <= 'L')
UNION ALL
(SELECT *
FROM colegio_p600_1300
WHERE ciudad >= 'A' AND ciudad <= 'L')
UNION ALL
(SELECT *
FROM colegio_p1300
WHERE ciudad >= 'A' AND ciudad <= 'L')
);

--- Partición intermedia 2: M-Z
CREATE TABLE colegio_frag_intermedia2 AS
(
(SELECT *
FROM colegio_p600
WHERE ciudad >= 'M' AND ciudad <= 'Z')
UNION ALL
(SELECT *
FROM colegio_p600_1300
WHERE ciudad >= 'M' AND ciudad <= 'Z')
UNION ALL
(SELECT *
FROM colegio_p1300
WHERE ciudad >= 'M' AND ciudad <= 'Z')
);

--- Query a partir de la fragmentación intermedia
SELECT *
FROM (
  (SELECT *
  FROM colegio_frag_intermedia1
  ORDER BY ciudad)
  UNION ALL
  (SELECT *
  FROM colegio_frag_intermedia2
  ORDER BY ciudad)
) AS subquery

--- Borramos las tablas intermedias
DROP TABLE colegio_frag_intermedia1;
DROP TABLE colegio_frag_intermedia2;

-- c)

--- Partición intermedia 1: Nivel académico Primaria
CREATE TABLE colegio_frag_intermedia1 AS
((SELECT *
FROM colegio_p600
WHERE nivel_acad IN ('Primaria'))
UNION ALL
(SELECT *
FROM colegio_p600_1300
WHERE nivel_acad IN ('Primaria'))
UNION ALL
(SELECT *
FROM colegio_p1300
WHERE nivel_acad IN ('Primaria'))
);

--- Partición intermedia 2: Nivel académico Secundaria
CREATE TABLE colegio_frag_intermedia2 AS
((SELECT *
FROM colegio_p600
WHERE nivel_acad IN ('Secundaria'))
UNION ALL
(SELECT *
FROM colegio_p600_1300
WHERE nivel_acad IN ('Secundaria'))
UNION ALL
(SELECT *
FROM colegio_p1300
WHERE nivel_acad IN ('Secundaria'))
);

--- Query a partir de fragmentación intermedia
SELECT *
FROM (
  (SELECT *
  FROM colegio_frag_intermedia1
  ORDER BY nivel_acad)
  UNION ALL
  (SELECT *
  FROM colegio_frag_intermedia2
  ORDER BY nivel_acad)
) AS subquery;

--- Eliminamos tablas intermedias
DROP TABLE colegio_frag_intermedia1;
DROP TABLE colegio_frag_intermedia2;


-- d

--Fragmentación intermedia 1

CREATE TABLE suma_conteo_R1 AS
(SELECT SUM(total_alumnos) as suma, COUNT(total_alumnos) as conteo
FROM colegio_p600);

--Fragmentación intermedia 2

CREATE TABLE suma_conteo_R2 AS
(SELECT SUM(total_alumnos) as suma, COUNT(total_alumnos) as conteo
FROM colegio_p600_1300);

--Fragmentación intermedia 3
CREATE TABLE suma_conteo_R3 AS
(SELECT SUM(total_alumnos) as suma, COUNT(total_alumnos) as conteo
FROM colegio_p1300);

-- Append de las 3 tuplas en el servidor central

SELECT sumaTotal/conteoTotal as promedio --transmisión de únicamente 3 tuplas al servidor central
FROM (
	SELECT SUM(suma) as sumaTotal, SUM(conteo) as conteoTotal
	FROM(
		SELECT * FROM suma_conteo_R1
		UNION ALL
		SELECT * FROM suma_conteo_R2
		UNION ALL
		SELECT * FROM suma_conteo_R3
	) as append_sumas_conteos
) as subconsulta;

--- Elimnación de fragmentos intermedios
DROP TABLE suma_conteo_r1;
DROP TABLE suma_conteo_r2;
DROP TABLE suma_conteo_r3;


-- e

--- Particiono en 2 fragmentos intermedios por rango

--- Partición intermedia 1: A-L
CREATE TABLE colegio_frag_intermedia1 AS
(
(SELECT *
FROM colegio_p600
WHERE ciudad >= 'A' AND ciudad <= 'L')
UNION ALL
(SELECT *
FROM colegio_p600_1300
WHERE ciudad >= 'A' AND ciudad <= 'L')
UNION ALL
(SELECT *
FROM colegio_p1300
WHERE ciudad >= 'A' AND ciudad <= 'L')
);

--- Partición intermedia 2: M-Z
CREATE TABLE colegio_frag_intermedia2 AS
(
(SELECT *
FROM colegio_p600
WHERE ciudad >= 'M' AND ciudad <= 'Z')
UNION ALL
(SELECT *
FROM colegio_p600_1300
WHERE ciudad >= 'M' AND ciudad <= 'Z')
UNION ALL
(SELECT *
FROM colegio_p1300
WHERE ciudad >= 'M' AND ciudad <= 'Z')
);

--- Como están fragmentando por rango, puedo agrupar sin generar grupos repetidos en ambos fragmentos

SELECT ciudad, suma_total --servidor central solo aplicaría suma
FROM (
	(SELECT ciudad, sum(total_alumnos) as suma_total
	FROM colegio_frag_intermedia1
	GROUP BY ciudad)
	UNION ALL
	(SELECT ciudad, sum(total_alumnos) as suma_total
	FROM colegio_frag_intermedia2
	GROUP BY ciudad)
) as subconsulta;

--- Eliminamos fragmentaciones intermedias
DROP TABLE colegio_frag_intermedia1;
DROP TABLE colegio_frag_intermedia2;