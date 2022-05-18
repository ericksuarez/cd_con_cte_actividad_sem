-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Afore
-- Creamos la tabla con el estatus de actividad/perdidos de Afore
-- Necesitamos tener la vista de 12 semanas antes del cliente para obtener la bandera de vista anual

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_afr_sem
partition(num_periodo_sem)
WITH 
	PIVOTE AS (
		-- Obtenemos los clientes que han estado activos en Afore desde hace 52 semanas (los que no han estado activos no apareceran en ese mes)
		SELECT 
			 afr.id_master
			,semanas.num_periodo_sem
		FROM (
			SELECT DISTINCT id_master
			FROM cd_baz_bdclientes.cd_afr_cte_hist
			WHERE id_master IS NOT NULL AND 
				  num_periodo_mes BETWEEN ${num_periodo_mes12} AND ${num_periodo_mes}
			) AS afr
		CROSS JOIN (
			SELECT DISTINCT num_periodo_sem
			FROM cd_baz_bdclientes.cd_gen_fechas_cat
			WHERE num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}
			) AS semanas
	)
	,ESTATUS AS (
		SELECT 
			 pvt.id_master
			,pvt.num_periodo_sem
			,COALESCE(ind_activo_afr, 0) AS ind_activo_afr
			,CASE WHEN coalesce(afr.ind_activo_afr,0) = 0
				    THEN count(*) OVER(PARTITION BY pvt.id_master,coalesce(afr.ind_activo_afr,0) ORDER BY pvt.num_periodo_sem ) 
				    ELSE 0
				    END AS num_sem_inact													
			,COALESCE(
				CASE WHEN MONTH(ant.fec_afr) = 1 AND WEEKOFYEAR(ant.fec_afr) >= 50 
					THEN CAST(CONCAT ( CAST(YEAR(ant.fec_afr) - 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_afr) AS STRING), 2, '0') ) AS INT) 
				  WHEN MONTH(ant.fec_afr) = 12 AND WEEKOFYEAR(ant.fec_afr) = 1 
					THEN CAST(CONCAT ( CAST(YEAR(ant.fec_afr) + 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_afr) AS STRING), 2, '0') ) AS INT) 
				  ELSE CAST(CONCAT ( CAST(YEAR(ant.fec_afr) AS STRING) ,LPAD(CAST(WEEKOFYEAR(ant.fec_afr) AS STRING), 2, '0') ) AS INT) 
				END			
				, pvt.num_periodo_sem) AS fec_ant_afr 
		FROM PIVOTE AS pvt
		LEFT JOIN (
			SELECT DISTINCT id_master
				,1 AS ind_activo_afr
			FROM cd_baz_bdclientes.cd_afr_cte_hist
			WHERE num_periodo_mes BETWEEN ${num_periodo_mes6} AND ${num_periodo_mes}
			) AS afr
			ON pvt.id_master = afr.id_master
		LEFT JOIN cd_baz_bdclientes.cd_con_cte_antiguedad AS ant
			ON pvt.id_master = ant.id_master
	)
	,ACTIVIDAD AS (
		SELECT 
			 id_master
			,num_periodo_sem
			,ind_activo_afr
			,num_sem_inact
			,IF(num_sem_inact = 40, 1, 0) AS ind_perdido_afr
			,CASE 
				WHEN est.ind_activo_afr = 1 AND LAG(est.ind_activo_afr, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_afr = 1 AND LAG(est.ind_activo_afr, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_afr >= est.num_periodo_sem
					THEN '2. Nuevo'
				WHEN est.ind_activo_afr = 1 AND LAG(est.ind_activo_afr, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_afr < est.num_periodo_sem
					THEN '3. Reactivado'
				WHEN est.ind_activo_afr = 0 AND LAG(est.ind_activo_afr, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '4. Inactivado'
				ELSE 'Sin Categoría Semanal'
				END AS cod_estatus_semanal_afr
			,CASE 
				WHEN est.ind_activo_afr = 1 AND LAG(est.ind_activo_afr, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_afr = 1 AND LAG(est.ind_activo_afr, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_afr >= ${num_periodo_sem53}
					THEN '2. Nuevo'
				WHEN est.ind_activo_afr = 1 AND LAG(est.ind_activo_afr, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_afr < ${num_periodo_sem53}
					THEN '3. Reactivado'
				WHEN est.ind_activo_afr = 0 AND LAG(est.ind_activo_afr, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1 AND est.num_sem_inact <= 39
					THEN '4. Inactivado'
				WHEN est.ind_activo_afr = 0 AND LAG(est.ind_activo_afr, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1 AND est.num_sem_inact >= 40
					THEN '5. Perdido'
				ELSE 'Sin Categoría 52s'
				END AS cod_estatus_52s_afr
		FROM ESTATUS AS est
	)
SELECT act.id_master
      ,act.ind_activo_afr
      ,act.ind_perdido_afr
      ,CAST(act.num_sem_inact AS INT) AS num_sem_inact
      ,act.cod_estatus_semanal_afr
      ,act.cod_estatus_52s_afr
      ,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}
;