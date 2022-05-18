-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- TOR
-- Creamos la tabla con el estatus de actividad/perdidos de TOR
-- Necesitamos tener la vista de 52 semanas antes del cliente para obtener la bandera de vista 52sem

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_tor_sem
partition(num_periodo_sem)

WITH 
	 SEMANAS_CAT AS (
		SELECT 
			 NUM_PERIODO_SEM
			,ROW_NUMBER() OVER (ORDER BY num_periodo_sem) ID
		FROM cd_baz_bdclientes.cd_gen_fechas_cat
		GROUP BY NUM_PERIODO_SEM
		)
	,ULT_DIA AS (
	-- Obtenemos el catálogo de semanas con su último día
	SELECT 
		 num_periodo_sem
		,MAX(fec_date) AS ult_dia_sem
		,MAX(fec_string) AS ult_dia_string
	FROM cd_baz_bdclientes.cd_gen_fechas_cat
	WHERE num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}
	GROUP BY num_periodo_sem
	)
	,PIVOTE AS (
	-- Obtenemos los clientes que han estado activos en TOR desde hace 52 semanas (los que no han estado activos no apareceran en ese mes)
	SELECT 
		 tor.id_master
		,semanas.num_periodo_sem
	FROM (
		SELECT DISTINCT id_master
		FROM rd_baz_bdclientes.rd_mplsdet4 AS rd
		INNER JOIN ULT_DIA -- Filtramos los días de corte ocupados
			ON rd.dat_ope = ult_dia.ult_dia_string
		LEFT JOIN (
			SELECT id_cliente
				,id_master
			FROM cd_baz_bdclientes.cd_cte_master
			WHERE cod_tipo_cliente = 'CLIENTE_ALNOVA'
			) AS master
			ON rd.num_cus = master.id_cliente
		WHERE TRIM(rd.bincrd_crdmx) IN ('458909', '516583') --Solo BINES de TOR
			AND rd.sem_atraso <= 39 -- Semanas de atraso menores a 40
			AND rd.saldo_cte_cap > 0
		) AS tor -- Con saldo vigente
	CROSS JOIN (
		SELECT num_periodo_sem
			,MAX(fec_date) AS ult_dia_sem
		FROM cd_baz_bdclientes.cd_gen_fechas_cat
		WHERE num_periodo_sem BETWEEN ${num_periodo_sem60}
				AND ${num_periodo_sem}
		GROUP BY num_periodo_sem
		) AS semanas
	)
	,ANT AS (
		SELECT 
			 pivote.id_master
			,MIN(COALESCE(plast.fec_tor_sem, ${num_periodo_sem})) AS fec_tor_sem
		FROM PIVOTE
		LEFT JOIN (
			SELECT 
				 id_cliente
				,id_master
			FROM cd_baz_bdclientes.cd_cte_master
			WHERE cod_tipo_cliente = 'CLIENTE_ALNOVA'
			) AS master
			ON pivote.id_master = master.id_master
		LEFT JOIN (
			SELECT 
				 num_cus
				,CASE WHEN MONTH(t028_dat_conreg) = 1 AND WEEKOFYEAR(t028_dat_conreg) >= 50 THEN CAST(CONCAT ( CAST(YEAR(t028_dat_conreg) - 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(t028_dat_conreg) AS STRING), 2, '0') ) AS INT) WHEN MONTH(t028_dat_conreg) = 12 AND WEEKOFYEAR(t028_dat_conreg) = 1 THEN CAST(CONCAT ( CAST(YEAR(t028_dat_conreg) + 1 AS STRING) ,LPAD(CAST(WEEKOFYEAR(t028_dat_conreg) AS STRING), 2, '0') ) AS INT) ELSE CAST(CONCAT ( CAST(YEAR(t028_dat_conreg) AS STRING) ,LPAD(CAST(WEEKOFYEAR(t028_dat_conreg) AS STRING), 2, '0') ) AS INT) 
						 END AS fec_tor_sem
			FROM cu_baz_bdclientes.cu_cd_mpg_plasticos_hist
			WHERE num_periodo_sem = ${num_periodo_sem}
			) AS plast
			ON plast.num_cus = master.id_cliente
		GROUP BY pivote.id_master
	)
	,PREVIA_ESTATUS AS (
		SELECT
			 A.id_master
			,A.num_periodo_sem
			,A.ind_sld
			,A.num_sem_atraso
			,(sc1.id - sc.id) AS num_sem_sld_0
			,A.fec_ant_tor 
			,a.max_periodo_sem
		FROM( 
			-- Calculamos los semanas que ha tenido saldo = $0 desde la última vez que tuvo saldo > $0, y obtenemos la antigüedad
			SELECT 
				 pvt.id_master
				,pvt.num_periodo_sem
				,max(pvt.num_periodo_sem*(COALESCE(tor.ind_sld, 0))) OVER(PARTITION BY pvt.id_master) as max_periodo_sem
				,coalesce(tor.ind_sld,0) AS ind_sld
				,tor.num_sem_atraso
				,CASE WHEN coalesce(tor.ind_sld,0) = 0
				    THEN count(*) OVER(PARTITION BY pvt.id_master,coalesce(tor.ind_sld,0) ORDER BY pvt.num_periodo_sem ) 
				    ELSE 0
				    END AS num_sem_sld_0 
				,COALESCE(fec_tor_sem, pvt.num_periodo_sem) AS fec_ant_tor
			FROM PIVOTE AS pvt
			LEFT JOIN (
				SELECT 
					 master.id_master
					,ult_dia.num_periodo_sem
					,CAST(MAX(rd.sem_atraso) AS BIGINT) AS num_sem_atraso
					,IF(SUM(COALESCE(rd.saldo_cte_cap, 0)) > 0, 1, 0) AS ind_sld
				FROM rd_baz_bdclientes.rd_mplsdet4 AS rd
				INNER JOIN ULT_DIA
					ON rd.dat_ope = ult_dia.ult_dia_string
				LEFT JOIN (
					SELECT id_cliente
						,id_master
					FROM cd_baz_bdclientes.cd_cte_master
					WHERE cod_tipo_cliente = 'CLIENTE_ALNOVA'
					) AS master
					ON rd.num_cus = master.id_cliente
				WHERE TRIM(rd.bincrd_crdmx) IN ('458909', '516583')  --Solo BINES de TOR
				GROUP BY master.id_master
					,ult_dia.num_periodo_sem
				) AS tor	ON 
			pvt.id_master = tor.id_master AND 
			pvt.num_periodo_sem = tor.num_periodo_sem
			-- Obtenemos la antigüedad
			LEFT JOIN ANT AS ant
				ON pvt.id_master = ant.id_master
			---WHERE pvt.ID_MASTER = 33520822
			) A
		LEFT JOIN SEMANAS_CAT AS SC ON
                SC.num_periodo_sem = a.max_periodo_sem
        LEFT JOIN SEMANAS_CAT AS SC1 ON
                SC1.num_periodo_sem = a.num_periodo_sem
	)
	,ESTATUS AS (
		-- Obtenemos los estatus de activo, inactivo y perdido
		SELECT
			 id_master
			,num_periodo_sem
			,ind_sld
			,num_sem_atraso
			,num_sem_sld_0
			,fec_ant_tor
			,CASE 
				WHEN ind_sld = 1 AND num_sem_atraso <= 39
					THEN 1
				ELSE 0
				END AS ind_activo_tor
			,CASE 
				WHEN ind_sld = 0 AND num_sem_sld_0 <= 8
					THEN 1
				ELSE 0
				END AS ind_inactivo_tor
			,CASE 
				WHEN num_sem_atraso > 39 AND LAG(num_sem_atraso, 1) OVER (
						PARTITION BY id_master ORDER BY num_periodo_sem
						) <= 39
					THEN 1
				WHEN ind_sld = 0 AND num_sem_sld_0 = 9
					THEN 1
				ELSE 0
				END AS ind_perdido_tor
		FROM PREVIA_ESTATUS
	)
	,ACTIVIDAD AS (
		-- Calculamos las marcas de estatus semanales y 52 semanas
		SELECT 
			 id_master
			,num_periodo_sem
			,COALESCE(ind_activo_tor, 0)   AS ind_activo_tor
			,COALESCE(ind_inactivo_tor, 0) AS ind_inactivo_tor
			,COALESCE(ind_perdido_tor, 0)  AS ind_perdido_tor
			,num_sem_atraso
			,num_sem_sld_0
			,CASE 
				WHEN est.ind_activo_tor = 1 AND LAG(est.ind_activo_tor, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_tor = 1 AND LAG(est.ind_activo_tor, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_tor >= est.num_periodo_sem
					THEN '2. Nuevo'
				WHEN est.ind_activo_tor = 1 AND LAG(est.ind_activo_tor, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_tor < est.num_periodo_sem
					THEN '3. Reactivado'
				WHEN est.ind_inactivo_tor = 1 AND LAG(est.ind_activo_tor, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '4. Inactivado'
				WHEN est.ind_perdido_tor = 1 AND LAG(est.ind_activo_tor, 1) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '5. Perdido'
				ELSE 'Sin Categoría Semanal'
				END AS cod_estatus_semanal_tor
			,CASE 
				WHEN est.ind_activo_tor = 1 AND LAG(est.ind_activo_tor, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '1. Se Mantiene Activo'
				WHEN est.ind_activo_tor = 1 AND LAG(est.ind_activo_tor, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_tor > ${num_periodo_sem53}
					THEN '2. Nuevo'
				WHEN est.ind_activo_tor = 1 AND LAG(est.ind_activo_tor, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 0 AND est.fec_ant_tor <= ${num_periodo_sem53}
					THEN '3. Reactivado'
				WHEN est.ind_inactivo_tor = 1 AND LAG(est.ind_activo_tor, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '4. Inactivado'
				WHEN (est.ind_perdido_tor = 1 OR (COALESCE(est.ind_activo_tor, 0) + COALESCE(est.ind_perdido_tor, 0) + COALESCE(est.ind_inactivo_tor, 0) = 0)) AND LAG(est.ind_activo_tor, 52) OVER (
						PARTITION BY est.id_master ORDER BY est.num_periodo_sem
						) = 1
					THEN '5. Perdido'
				ELSE 'Sin Categoría 52sem'
				END AS cod_estatus_52s_tor
		FROM ESTATUS AS est
	)
SELECT 
	 act.id_master
	,CAST(act.ind_activo_tor AS TINYINT) AS IND_ACTIVO_TOR
	,CAST(ind_inactivo_tor AS TINYINT) AS IND_INACTIVO_TOR
	,CAST(act.ind_perdido_tor AS TINYINT) AS IND_PERDIDO_TOR
	,CAST(act.num_sem_atraso AS INT) AS NUM_SEM_ATRASO
	,CAST(act.num_sem_sld_0 AS INT) AS NUM_SEM_SLD_0
	,act.cod_estatus_semanal_tor
	,act.cod_estatus_52s_tor
	,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}
;