--DROP TABLE colegio;

CREATE TABLE COLEGIO (
	Codigo INTEGER,
	Nombre VARCHAR(250),
	Direccion VARCHAR(250),
	Ciudad VARCHAR(250),
	NivelAcademico VARCHAR(250),
	TotalAlumnos INTEGER
) PARTITION BY RANGE (TotalAlumnos);


-- Crear conexión con servidores 1 y 2

---DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER Server1;
---DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER Server2;

---DROP SERVER IF EXISTS Server1;
---DROP SERVER IF EXISTS Server2;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER Server1 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'snuffleupagus.db.elephantsql.com', dbname 'quwgujjc', port '5432');


CREATE SERVER Server2 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'snuffleupagus.db.elephantsql.com', dbname 'aleacaxt', port '5432');


CREATE USER MAPPING FOR CURRENT_USER SERVER Server1 OPTIONS (user 'quwgujjc', password 'yn1Rcjhf6J4BUJCttE0ZVjMXJepNFhCF');

CREATE USER MAPPING FOR CURRENT_USER SERVER Server2 OPTIONS (user 'aleacaxt', password 'n5zymHNogH_WgtqFVGtBlB5t2s7XFMTx');

-- Crear particiones utilizando servidores 1 y 2

CREATE TABLE COLEGIO_1 PARTITION OF COLEGIO FOR VALUES FROM (1) to (600);
CREATE FOREIGN TABLE public.COLEGIO_2 PARTITION OF COLEGIO FOR VALUES FROM (600) to (1300) SERVER Server1;
CREATE FOREIGN TABLE public.COLEGIO_3 PARTITION OF COLEGIO FOR VALUES FROM (1300) to (3001) SERVER Server2;

-- Copiar datos a la tabla
--- Desde linea de comandos psql: \copy COLEGIO (Codigo, Nombre, Direccion, Ciudad, NivelAcademico, TotalAlumnos) FROM 'C:\Users\ASUS\Downloads\datos_lab15.csv' WITH (FORMAT csv, HEADER true);


-- Consultas

--- a

---- No optimizada
SELECT * FROM colegio ORDER BY TotalAlumnos;

---- Optimizada
SELECT *
FROM (
  (SELECT *
  FROM COLEGIO_1
  ORDER BY TotalAlumnos)
  UNION ALL
  (SELECT *
  FROM COLEGIO_2
  ORDER BY TotalAlumnos)
  UNION ALL
  (SELECT *
  FROM COLEGIO_3
  ORDER BY TotalAlumnos)
) AS subquery;

---b 

---- No optimizada
Select * From Colegio Order By Ciudad;

---- Optimizada
SELECT *
FROM (
  (SELECT *
  FROM ((SELECT *
FROM COLEGIO_1
WHERE ciudad >= 'A' AND ciudad <= 'L')
UNION ALL
(SELECT *
FROM COLEGIO_2
WHERE ciudad >= 'A' AND ciudad <= 'L')
UNION ALL
(SELECT *
FROM COLEGIO_3
WHERE ciudad >= 'A' AND ciudad <= 'L')) as fragmentacion_intermedia1
  ORDER BY ciudad)
  UNION ALL
  (SELECT *
  FROM ((SELECT *
FROM COLEGIO_1
WHERE ciudad >= 'M' AND ciudad <= 'Z')
UNION ALL
(SELECT *
FROM COLEGIO_2
WHERE ciudad >= 'M' AND ciudad <= 'Z')
UNION ALL
(SELECT *
FROM COLEGIO_3
WHERE ciudad >= 'M' AND ciudad <= 'Z')) as fragmentacion_intermedia2
  ORDER BY ciudad)
) AS subquery

--- c

----No optimizada
Select * From Colegio Order By NivelAcademico;


---- Optimizada

SELECT *
FROM (
  (SELECT *
  FROM (
	(SELECT *
	FROM COLEGIO_1
	WHERE NivelAcademico IN ('Primaria'))
	UNION ALL
	(SELECT *
	FROM COLEGIO_2
	WHERE NivelAcademico IN ('Primaria'))
	UNION ALL
	(SELECT *
	FROM COLEGIO_3
	WHERE NivelAcademico IN ('Primaria'))
	) AS colegio_frag_intermedia1
  ORDER BY NivelAcademico)
  UNION ALL
  (SELECT *
  FROM (
	 (SELECT *
	FROM COLEGIO_1
	WHERE NivelAcademico IN ('Secundaria'))
	UNION ALL
	(SELECT *
	FROM COLEGIO_2
	WHERE NivelAcademico IN ('Secundaria'))
	UNION ALL
	(SELECT *
	FROM COLEGIO_3
	WHERE NivelAcademico IN ('Secundaria'))
	) AS colegio_frag_intermedia2
  ORDER BY NivelAcademico)
) AS subquery;

--- d

---- No optimizada
Select AVG(TotalAlumnos) From Colegio;

---- Optimizada
SELECT sumaTotal/conteoTotal as promedio
FROM (
	SELECT SUM(suma) as sumaTotal, SUM(conteo) as conteoTotal
	FROM(
		SELECT * FROM ((SELECT SUM(TotalAlumnos) as suma, COUNT(TotalAlumnos) as conteo
FROM COLEGIO_1))AS suma_conteo_R1
		UNION ALL
		SELECT * FROM (SELECT SUM(TotalAlumnos) as suma, COUNT(TotalAlumnos) as conteo
FROM COLEGIO_2) AS suma_conteo_R2
		UNION ALL
		SELECT * FROM (SELECT SUM(TotalAlumnos) as suma, COUNT(TotalAlumnos) as conteo
FROM COLEGIO_3) AS suma_conteo_R3
	) as append_sumas_conteos
) as subconsulta;


--- e

---- No optimizado

Select Ciudad, SUM(TotalAlumnos) From Colegio Group By Ciudad;


---- Optimizado

SELECT ciudad, suma_total --servidor central solo aplicaría suma
FROM (
	(SELECT ciudad, sum(TotalAlumnos) as suma_total
	FROM (
		(SELECT *
		FROM COLEGIO_1
		WHERE ciudad >= 'A' AND ciudad <= 'L')
		UNION ALL
		(SELECT *
		FROM COLEGIO_2
		WHERE ciudad >= 'A' AND ciudad <= 'L')
		UNION ALL
		(SELECT *
		FROM COLEGIO_3
		WHERE ciudad >= 'A' AND ciudad <= 'L')
		) AS colegio_frag_intermedia1
	GROUP BY ciudad)
	UNION ALL
	(SELECT ciudad, sum(TotalAlumnos) as suma_total
	FROM (
		(SELECT *
		FROM COLEGIO_1
		WHERE ciudad >= 'M' AND ciudad <= 'Z')
		UNION ALL
		(SELECT *
		FROM COLEGIO_2
		WHERE ciudad >= 'M' AND ciudad <= 'Z')
		UNION ALL
		(SELECT *
		FROM COLEGIO_3
		WHERE ciudad >= 'M' AND ciudad <= 'Z')
		) AS colegio_frag_intermedia2
	GROUP BY ciudad)
) as subconsulta;

