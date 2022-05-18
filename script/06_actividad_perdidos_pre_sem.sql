-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Prendario
-- Creamos la tabla con el estatus de actividad/perdidos de Prendario
-- Necesitamos tener la vista de 52 semanas antes del cliente para obtener la bandera de vista anual

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_pre_sem
partition(num_periodo_sem)

WITH PIVOTE AS (
-- Obtenemos los clientes que han estado activos en Prendario desde hace 52 semanas (los que no han estado activos no apareceran en esa semana)
SELECT pre.id_master, semanas.num_periodo_sem, semanas.id
FROM
(SELECT DISTINCT hist.id_master
 FROM ${esquema_cd}.cd_pre_boletaoro_hist_sem AS hist
	LEFT JOIN ${esquema_cd}.cd_pre_boletaoro_sem AS boleta
		ON hist.id_boleta = boleta.id_boleta
 WHERE hist.sld_capital > 0 -- Saldo de la boleta > $0 al final de la semana
   --AND boleta.id_producto != 9 -- Quitando boletas del producto express
   AND hist.num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS pre

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
		  ,a.ind_activo_pre
		  ,a.id - fec_cat.id as num_sem_inact
		  ,a.fec_ant_pre
	FROM
      (SELECT 
		 pvt.id_master
		,pvt.num_periodo_sem
		,pvt.id
        ,COALESCE(ind_activo_pre, 0) AS ind_activo_pre
		,max(pvt.num_periodo_sem * (COALESCE(pre.ind_activo_pre, 0))) OVER(PARTITION BY pvt.id_master) as max_periodo_sem
        ,COALESCE((CASE WHEN MONTH(ant.fec_pre) = 1 AND WEEKOFYEAR(ant.fec_pre) >= 50 THEN CAST(CONCAT ( CAST(YEAR(ant.fec_pre) - 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_pre) AS STRING), 2, '0') ) AS INT) 
					   WHEN MONTH(ant.fec_pre) = 12 AND WEEKOFYEAR(ant.fec_pre) = 1  THEN CAST(CONCAT ( CAST(YEAR(ant.fec_pre) + 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_pre) AS STRING), 2, '0') ) AS INT) 
					   ELSE CAST(CONCAT ( CAST(YEAR(ant.fec_pre) AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_pre) AS STRING), 2, '0') ) AS INT) END),
				   pvt.num_periodo_sem) AS fec_ant_pre					  
      FROM PIVOTE AS pvt
		LEFT JOIN
		  (SELECT hist.id_master, 
				   hist.num_periodo_sem, 
				   1 AS ind_activo_pre
			FROM ${esquema_cd}.cd_pre_boletaoro_hist_sem AS hist
				LEFT JOIN ${esquema_cd}.cd_pre_boletaoro_sem as boleta
					ON hist.id_boleta = boleta.id_boleta
			WHERE hist.sld_capital > 0 -- Saldo de la boleta > $0 al final de la semana
					--AND boleta.id_producto != 9 -- Quitando boletas del producto express
					AND hist.num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}
			GROUP BY hist.id_master, hist.num_periodo_sem) AS pre
				ON pvt.id_master = pre.id_master AND
				   pvt.num_periodo_sem = pre.num_periodo_sem
					LEFT JOIN cd_baz_bdclientes.cd_con_cte_antiguedad AS ant  -- Cruce con antigüedad para tomar fec_pre
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
            ,ind_activo_pre
            ,num_sem_inact
            ,IF(num_sem_inact = 9, 1, 0) AS ind_perdido_pre
            ,CASE WHEN est.ind_activo_pre = 1 AND
                       LAG(est.ind_activo_pre, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_pre = 1 AND
                       LAG(est.ind_activo_pre, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pre >= est.num_periodo_sem THEN '2. Nuevo'
                  WHEN est.ind_activo_pre = 1 AND
                       LAG(est.ind_activo_pre, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pre < est.num_periodo_sem THEN '3. Reactivado'
                  WHEN est.ind_activo_pre = 0 AND
                       LAG(est.ind_activo_pre, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  ELSE 'Sin Categoría Semanal' 
			END AS cod_estatus_semanal_pre
            ,CASE WHEN est.ind_activo_pre = 1 AND
                       LAG(est.ind_activo_pre, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_pre = 1 AND
                       LAG(est.ind_activo_pre, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pre >= ${num_periodo_sem53} THEN '2. Nuevo'
                  WHEN est.ind_activo_pre = 1 AND
                       LAG(est.ind_activo_pre, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pre < ${num_periodo_sem53} THEN '3. Reactivado'
                  WHEN est.ind_activo_pre = 0 AND
                       LAG(est.ind_activo_pre, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact <= 8 THEN '4. Inactivado'
                  WHEN est.ind_activo_pre = 0 AND
                       LAG(est.ind_activo_pre, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact > 8 THEN '5. Perdido'
                  ELSE 'Sin Categoría 52sem' 
			END AS cod_estatus_52s_pre
      FROM ESTATUS AS est
)
SELECT act.id_master
      ,act.ind_activo_pre
      ,act.ind_perdido_pre
      ,cast(act.num_sem_inact as int) num_sem_inact
      ,act.cod_estatus_semanal_pre
      ,act.cod_estatus_52s_pre
	  ,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}

-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_pre_sem (
--       id_master                  BIGINT   
--      ,ind_activo_pre             INT
--      ,ind_perdido_pre            INT
--      ,num_sem_inact              INT
--      ,cod_estatus_semanal_pre    STRING
--      ,cod_estatus_52s_pre        STRING
--		,num_periodo_sem            INT
-- ) STORED AS PARQUET;