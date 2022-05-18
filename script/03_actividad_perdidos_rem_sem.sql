-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Remesas
-- Creamos la tabla con el estatus de actividad/perdidos de Remesas
-- Necesitamos tener la vista de 52 semanas antes del cliente para obtener la bandera de vista anual

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_rem_sem
partition(num_periodo_sem)

WITH PIVOTE AS (
-- Obtenemos los clientes que han estado activos en Remesas desde hace 52 semanas (los que no han estado activos no apareceran en esa semana)
SELECT rem.id_master, semanas.num_periodo_sem, semanas.id
FROM
(SELECT DISTINCT id_master
 FROM ${esquema_cd}.cd_env_cte_hist_sem
 WHERE id_master IS NOT NULL
   AND ind_activo_rem = 1
   AND num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS rem

CROSS JOIN
(SELECT DISTINCT num_periodo_sem, id
 FROM (SELECT num_periodo_sem
             ,ROW_NUMBER() OVER (ORDER BY num_periodo_sem) ID
       FROM cd_baz_bdclientes.cd_gen_fechas_cat
       GROUP BY num_periodo_sem) fec_cat
 WHERE num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS semanas
)

, ESTATUS AS (
	SELECT a.id_master
		  ,a.num_periodo_sem
		  ,a.ind_activo_rem
		  ,a.id - fec_cat.id as num_sem_inact
		  ,a.fec_ant_rem
	FROM 
      (SELECT pvt.id_master
			 ,pvt.num_periodo_sem
			 ,pvt.id
             ,COALESCE(rem.ind_activo_rem, 0) AS ind_activo_rem
			 ,max(pvt.num_periodo_sem * (COALESCE(rem.ind_activo_rem, 0))) OVER(PARTITION BY pvt.id_master) as max_periodo_sem
			 ,COALESCE((CASE WHEN MONTH(ant.fec_env) = 1 AND WEEKOFYEAR(ant.fec_env) >= 50 THEN CAST(CONCAT ( CAST(YEAR(ant.fec_env) - 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_env) AS STRING), 2, '0') ) AS INT) 
							 WHEN MONTH(ant.fec_env) = 12 AND WEEKOFYEAR(ant.fec_env) = 1  THEN CAST(CONCAT ( CAST(YEAR(ant.fec_env) + 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_env) AS STRING), 2, '0') ) AS INT) 
							 ELSE CAST(CONCAT ( CAST(YEAR(ant.fec_env) AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_env) AS STRING), 2, '0') ) AS INT) END),
						pvt.num_periodo_sem) AS fec_ant_rem					  
      FROM PIVOTE AS pvt
		LEFT JOIN ${esquema_cd}.cd_env_cte_hist_sem AS rem
			ON pvt.id_master = rem.id_master AND
			   pvt.num_periodo_sem = rem.num_periodo_sem
		LEFT JOIN cd_baz_bdclientes.cd_con_cte_antiguedad AS ant
			ON pvt.id_master = ant.id_master) a
		INNER JOIN (SELECT num_periodo_sem
								  ,ROW_NUMBER() OVER (ORDER BY num_periodo_sem) id
						    FROM cd_baz_bdclientes.cd_gen_fechas_cat
						    GROUP BY num_periodo_sem) fec_cat
					ON a.max_periodo_sem = fec_cat.num_periodo_sem
)

, ACTIVIDAD AS (
      SELECT id_master
            ,num_periodo_sem
            ,ind_activo_rem
            ,num_sem_inact
            ,IF(num_sem_inact = 40, 1, 0) AS ind_perdido_rem
            ,CASE WHEN est.ind_activo_rem = 1 AND
                       LAG(est.ind_activo_rem, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_rem = 1 AND
                       LAG(est.ind_activo_rem, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_rem >= est.num_periodo_sem THEN '2. Nuevo'
                  WHEN est.ind_activo_rem = 1 AND
                       LAG(est.ind_activo_rem, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_rem < est.num_periodo_sem THEN '3. Reactivado'
                  WHEN est.ind_activo_rem = 0 AND
                       LAG(est.ind_activo_rem, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  ELSE 'Sin Categoría Semanal' 
			 END AS cod_estatus_semanal_rem
            ,CASE WHEN est.ind_activo_rem = 1 AND
                       LAG(est.ind_activo_rem, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_rem = 1 AND
                       LAG(est.ind_activo_rem, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_rem >= ${num_periodo_sem53} THEN '2. Nuevo'
                  WHEN est.ind_activo_rem = 1 AND
                       LAG(est.ind_activo_rem, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_rem < ${num_periodo_sem53} THEN '3. Reactivado'
                  WHEN est.ind_activo_rem = 0 AND
                       LAG(est.ind_activo_rem, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact <= 39 THEN '4. Inactivado'
                  WHEN est.ind_activo_rem = 0 AND
                       LAG(est.ind_activo_rem, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact >= 40 THEN '5. Perdido'
                  ELSE 'Sin Categoría 52sem' 
			 END AS cod_estatus_52s_rem
      FROM ESTATUS AS est
)
SELECT act.id_master
      ,act.ind_activo_rem
      ,act.ind_perdido_rem
      ,cast(act.num_sem_inact as int) as num_sem_inact
      ,act.cod_estatus_semanal_rem
      ,act.cod_estatus_52s_rem
	  ,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}

-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_rem_sem (
--       id_master                  BIGINT
--      ,num_periodo_sem            INT
--      ,ind_activo_rem             INT
--      ,ind_perdido_rem            INT
--      ,num_sem_inact              INT
--      ,cod_estatus_semanal_rem    STRING
--      ,cod_estatus_52s_rem        STRING
-- ) STORED AS PARQUET;