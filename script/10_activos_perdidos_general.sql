--|************************************| PASO 01 |************************************|--
INSERT OVERWRITE TABLE ${esquema_cu}.cu_esb_con_cte_act_sem_pvt
SELECT 
	 pvt.id_master
	,ant.min_sem_act
	,ant.min_sem_act_sin_afr
	,pvt.num_periodo_sem
FROM(
	SELECT 
		 master.id_master
		,semanas.num_periodo_sem
	FROM (
		SELECT DISTINCT id_master
		FROM ${esquema_cu}.cu_con_cte_actividad_prev_sem
		WHERE num_periodo_sem IN (${num_periodo_sem},${num_periodo_sem1},${num_periodo_sem53})
		) AS master 
	CROSS JOIN(
		SELECT DISTINCT num_periodo_sem
		FROM ${esquema_cu}.cu_con_cte_actividad_prev_sem
		WHERE num_periodo_sem IN (${num_periodo_sem},${num_periodo_sem1},${num_periodo_sem53})
		) AS semanas
	) pvt
LEFT JOIN ${esquema_cu}.cu_con_cte_actividad_pivot_aux ant	ON 
	 pvt.id_master = ant.id_master AND 
	(ant.min_sem_act <= ${num_periodo_sem} OR ant.min_sem_act_sin_afr <= ${num_periodo_sem})
;
-- Inserted 68,995,408 row(s) 11s
COMPUTE STATS ${esquema_cu}.cu_esb_con_cte_act_sem_pvt;

--|************************************| PASO 02 |************************************|--
INSERT OVERWRITE TABLE ${esquema_cu}.cu_esb_con_cte_actividad_prev_sem
SELECT *
FROM ${esquema_cu}.cu_con_cte_actividad_prev_sem
WHERE num_periodo_sem IN (${num_periodo_sem},${num_periodo_sem1},${num_periodo_sem53})
;
-- Inserted 68,995,408 row(s) 33s
COMPUTE STATS ${esquema_cu}.cu_esb_con_cte_actividad_prev_sem;

--|************************************| PASO 03 |************************************|--
INSERT OVERWRITE TABLE ${esquema_cu}.cu_esb_con_cte_act_sem_core
SELECT 
	 pvt.id_master
	,pvt.num_periodo_sem
	,act.ind_activo_cap
	,act.ind_activo_capnom
	,act.ind_activo_credimax
	,act.ind_activo_nom
	,act.ind_activo_tor
	,act.ind_activo_cre
	,act.ind_activo_rem
	,act.ind_activo_dex
	,act.ind_activo_div
	,act.ind_activo_pre
	,act.ind_activo_pgs
	,act.ind_activo_afr
	,act.ind_perdido_cap
	,act.ind_perdido_capnom
	,act.ind_perdido_credimax
	,act.ind_perdido_nom
	,act.ind_perdido_tor
	,act.ind_perdido_cre
	,act.ind_perdido_rem
	,act.ind_perdido_dex
	,act.ind_perdido_div
	,act.ind_perdido_pre
	,act.ind_perdido_pgs
	,act.ind_perdido_afr
	,act.num_sem_sld_0_credimax
	,act.num_sem_sld_0_nom
	,act.num_sem_sld_0_tor
	,act.num_sem_sld_0_cre
	,act.num_sem_inact_rem
	,act.num_sem_inact_dex
	,act.num_sem_inact_div
	,act.num_sem_inact_pre
	,act.num_sem_inact_pgs
	,act.num_sem_inact_afr
	,act.ind_inactivo_cap
	,act.ind_inactivo_capnom
	,act.ind_inactivo_credimax
	,act.ind_inactivo_nom
	,act.ind_inactivo_tor
	,act.ind_inactivo_cre
	,act.ind_inactivo_rem
	,act.ind_inactivo_dex
	,act.ind_inactivo_div
	,act.ind_inactivo_pre
	,act.ind_inactivo_pgs
	,act.ind_inactivo_afr
	,act.cod_estatus_semanal_cap
	,act.cod_estatus_semanal_capnom
	,act.cod_estatus_semanal_credimax
	,act.cod_estatus_semanal_nom
	,act.cod_estatus_semanal_tor
	,act.cod_estatus_semanal_cre
	,act.cod_estatus_semanal_rem
	,act.cod_estatus_semanal_dex
	,act.cod_estatus_semanal_div
	,act.cod_estatus_semanal_pre
	,act.cod_estatus_semanal_pgs
	,act.cod_estatus_semanal_afr
	,act.cod_estatus_52s_cap
	,act.cod_estatus_52s_capnom
	,act.cod_estatus_52s_credimax
	,act.cod_estatus_52s_nom
	,act.cod_estatus_52s_tor
	,act.cod_estatus_52s_cre
	,act.cod_estatus_52s_rem
	,act.cod_estatus_52s_dex
	,act.cod_estatus_52s_div
	,act.cod_estatus_52s_pre
	,act.cod_estatus_52s_pgs
	,act.cod_estatus_52s_afr
	,act.ind_activo
	,act.ind_activo_ctes_usua
	,act.ind_activo_solo_ctes
	,act.ind_activo_solo_usua
	,act.ind_activo_sin_afr
	,act.ind_activo_ctes_usua_sin_afr
	,act.ind_activo_solo_ctes_sin_afr
	,act.ind_activo_solo_usua_sin_afr
	,act.ind_inactivo
	,act.ind_inactivo_sin_afr
	,act.ind_perdido
	,act.ind_perdido_sin_afr
	,act.cod_cte_usua
	,act.cod_cte_usua_sin_afr
	,pvt.min_sem_act
	,pvt.min_sem_act_sin_afr
FROM ${esquema_cu}.cu_esb_con_cte_act_sem_pvt pvt
LEFT JOIN ${esquema_cu}.cu_esb_con_cte_actividad_prev_sem act ON 
	pvt.id_master       = act.id_master AND
	pvt.num_periodo_sem = act.num_periodo_sem
;
-- Inserted 68,995,408 row(s) 1m5s
COMPUTE STATS ${esquema_cu}.cu_esb_con_cte_act_sem_core;
   
--|************************************| PASO 04 |************************************|--
INSERT OVERWRITE TABLE ${esquema_cu}.cu_esb_con_cte_actividad_sem

SELECT 
	 id_master
	,ind_activo_cap
	,ind_activo_capnom
	,ind_activo_credimax
	,ind_activo_nom
	,ind_activo_tor
	,ind_activo_cre
	,ind_activo_rem
	,ind_activo_dex
	,ind_activo_div
	,ind_activo_pre
	,ind_activo_pgs
	,ind_activo_afr
	,ind_perdido_cap
	,ind_perdido_capnom
	,ind_perdido_credimax
	,ind_perdido_nom
	,ind_perdido_tor
	,ind_perdido_cre
	,ind_perdido_rem
	,ind_perdido_dex
	,ind_perdido_div
	,ind_perdido_pre
	,ind_perdido_pgs
	,ind_perdido_afr
	,num_sem_sld_0_credimax
	,num_sem_sld_0_nom
	,num_sem_sld_0_tor
	,num_sem_sld_0_cre
	,num_sem_inact_rem
	,num_sem_inact_dex
	,num_sem_inact_div
	,num_sem_inact_pre
	,num_sem_inact_pgs
	,num_sem_inact_afr
	,ind_inactivo_cap
	,ind_inactivo_capnom
	,ind_inactivo_credimax
	,ind_inactivo_nom
	,ind_inactivo_tor
	,ind_inactivo_cre
	,ind_inactivo_rem
	,ind_inactivo_dex
	,ind_inactivo_div
	,ind_inactivo_pre
	,ind_inactivo_pgs
	,ind_inactivo_afr
	,cod_estatus_semanal_cap
	,cod_estatus_semanal_capnom
	,cod_estatus_semanal_credimax
	,cod_estatus_semanal_nom
	,cod_estatus_semanal_tor
	,cod_estatus_semanal_cre
	,cod_estatus_semanal_rem
	,cod_estatus_semanal_dex
	,cod_estatus_semanal_div
	,cod_estatus_semanal_pre
	,cod_estatus_semanal_pgs
	,cod_estatus_semanal_afr
	,cod_estatus_52s_cap
	,cod_estatus_52s_capnom
	,cod_estatus_52s_credimax
	,cod_estatus_52s_nom
	,cod_estatus_52s_tor
	,cod_estatus_52s_cre
	,cod_estatus_52s_rem
	,cod_estatus_52s_dex
	,cod_estatus_52s_div
	,cod_estatus_52s_pre
	,cod_estatus_52s_pgs
	,cod_estatus_52s_afr
	,ind_activo
	,ind_activo_ctes_usua
	,ind_activo_solo_ctes
	,ind_activo_solo_usua
	,ind_activo_sin_afr
	,ind_activo_ctes_usua_sin_afr
	,ind_activo_solo_ctes_sin_afr
	,ind_activo_solo_usua_sin_afr
	,ind_inactivo
	,ind_inactivo_sin_afr
	,ind_perdido
	,ind_perdido_sin_afr
	,cod_cte_usua
	,cod_cte_usua_sin_afr
-- Códigos de Estatus generales con Afore
	,CASE 
		WHEN COALESCE(ind_activo, 0) = 1 AND COALESCE(LAG(ind_activo, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1
			THEN '1. Se Mantiene Activo'
		WHEN COALESCE(ind_activo, 0) = 1 AND COALESCE(LAG(ind_activo, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			 AND COALESCE(min_sem_act, ${num_periodo_sem}) >= ${num_periodo_sem}
			THEN '2. Nuevo' 
		WHEN COALESCE(ind_activo, 0) = 1 AND COALESCE(LAG(ind_activo, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			 AND COALESCE(min_sem_act, ${num_periodo_sem}) < ${num_periodo_sem}
			THEN '3. Reactivado'
		WHEN COALESCE(ind_activo, 0) = 0 AND COALESCE(LAG(ind_activo, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND COALESCE(ind_inactivo, 0) = 1
			THEN '4. Inactivado'
		WHEN COALESCE(ind_activo, 0) = 0 AND COALESCE(LAG(ind_activo, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND COALESCE(ind_perdido, 0) = 1
			THEN '5. Perdido'
		WHEN ind_activo IS NULL AND LAG(ind_activo, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem) = 1
			THEN '5. Perdido'
		ELSE 'Sin Categoría Semanal'
		END AS cod_estatus_semanal_gen
	,CASE 
		WHEN COALESCE(ind_activo, 0) = 1 AND COALESCE(LAG(ind_activo, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1
			THEN '1. Se Mantiene Activo'
		WHEN COALESCE(ind_activo, 0) = 1 AND COALESCE(LAG(ind_activo, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			 AND COALESCE(min_sem_act, ${num_periodo_sem}) >= ${num_periodo_sem53}
			THEN '2. Nuevo'
		WHEN COALESCE(ind_activo, 0) = 1 AND COALESCE(LAG(ind_activo, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			 AND COALESCE(min_sem_act, ${num_periodo_sem}) < ${num_periodo_sem53}
			THEN '3. Reactivado'
		WHEN COALESCE(ind_activo, 0) = 0 AND COALESCE(LAG(ind_activo, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND COALESCE(ind_inactivo, 0) = 1
			THEN '4. Inactivado'
		WHEN COALESCE(ind_activo, 0) = 0 AND COALESCE(LAG(ind_activo, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND (COALESCE(ind_perdido, 0) = 1 OR (COALESCE(ind_activo, 0) = 0 
			 AND COALESCE(ind_inactivo, 0) = 0 AND COALESCE(ind_perdido, 0) = 0)) 
			 AND LAG(cod_cte_usua, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem) = 'CLIENTE'
			THEN '5. Perdido Cliente'
		WHEN COALESCE(ind_activo, 0) = 0 AND COALESCE(LAG(ind_activo, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND (COALESCE(ind_perdido, 0) = 1 OR (COALESCE(ind_activo, 0) = 0 
			 AND COALESCE(ind_inactivo, 0) = 0 AND COALESCE(ind_perdido, 0) = 0)) 
			 AND LAG(cod_cte_usua, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem) = 'USUARIO'
			THEN '5. Perdido Usuario'
		ELSE 'Sin Categoría 52s'
		END AS cod_estatus_52s_gen
	-- Códigos de Estatus generales sin Afore
	,CASE 
		WHEN COALESCE(ind_activo_sin_afr, 0) = 1 AND COALESCE(LAG(ind_activo_sin_afr, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1
			THEN '1. Se Mantiene Activo'
		WHEN COALESCE(ind_activo_sin_afr, 0) = 1 AND COALESCE(LAG(ind_activo_sin_afr, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			 AND COALESCE(min_sem_act_sin_afr, ${num_periodo_sem}) >= ${num_periodo_sem}
			THEN '2. Nuevo' 
		WHEN COALESCE(ind_activo_sin_afr, 0) = 1 AND COALESCE(LAG(ind_activo_sin_afr, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			AND COALESCE(min_sem_act_sin_afr, ${num_periodo_sem}) < ${num_periodo_sem}
			THEN '3. Reactivado' 
		WHEN COALESCE(ind_activo_sin_afr, 0) = 0 AND COALESCE(LAG(ind_activo_sin_afr, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND COALESCE(ind_inactivo_sin_afr, 0) = 1
			THEN '4. Inactivado'
		WHEN COALESCE(ind_activo_sin_afr, 0) = 0 AND COALESCE(LAG(ind_activo_sin_afr, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND COALESCE(ind_perdido_sin_afr, 0) = 1
			THEN '5. Perdido'
		WHEN ind_activo_sin_afr IS NULL AND LAG(ind_activo_sin_afr, 1) OVER (PARTITION BY id_master ORDER BY num_periodo_sem) = 1
			THEN '5. Perdido' 
			ELSE 'Sin Categoría Semanal'
		END AS cod_estatus_semanal_gen_sin_afr
	,CASE 
		WHEN COALESCE(ind_activo_sin_afr, 0) = 1 AND COALESCE(LAG(ind_activo_sin_afr, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1
			THEN '1. Se Mantiene Activo'
		WHEN COALESCE(ind_activo_sin_afr, 0) = 1 AND COALESCE(LAG(ind_activo_sin_afr, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			 AND COALESCE(min_sem_act_sin_afr, ${num_periodo_sem}) >= ${num_periodo_sem53}
			THEN '2. Nuevo' 
		WHEN COALESCE(ind_activo_sin_afr, 0) = 1 AND COALESCE(LAG(ind_activo_sin_afr, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 
			 AND COALESCE(min_sem_act_sin_afr, ${num_periodo_sem}) < ${num_periodo_sem53}
			THEN '3. Reactivado' 
		WHEN COALESCE(ind_activo_sin_afr, 0) = 0 AND COALESCE(LAG(ind_activo_sin_afr, 2) OVER (PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 
			 AND COALESCE(ind_inactivo_sin_afr, 0) = 1
			THEN '4. Inactivado'
		WHEN COALESCE(ind_activo_sin_afr, 0) = 0 AND COALESCE(LAG(ind_activo_sin_afr, 2) OVER (
					PARTITION BY id_master ORDER BY num_periodo_sem
					), 0) = 1 AND (COALESCE(ind_perdido_sin_afr, 0) = 1 OR (COALESCE(ind_activo_sin_afr, 0) = 0 
					AND COALESCE(ind_inactivo_sin_afr, 0) = 0 
					AND COALESCE(ind_perdido_sin_afr, 0) = 0)) 
					AND LAG(cod_cte_usua_sin_afr, 2) OVER (
				PARTITION BY id_master ORDER BY num_periodo_sem
				) = 'CLIENTE'
			THEN '5. Perdido Cliente'
		WHEN COALESCE(ind_activo_sin_afr, 0) = 0 AND COALESCE(LAG(ind_activo_sin_afr, 2) OVER (
					PARTITION BY id_master ORDER BY num_periodo_sem
					), 0) = 1 AND (COALESCE(ind_perdido_sin_afr, 0) = 1 OR (COALESCE(ind_activo_sin_afr, 0) = 0 
					AND COALESCE(ind_inactivo_sin_afr, 0) = 0 
					AND COALESCE(ind_perdido_sin_afr, 0) = 0)) 
					AND LAG(cod_cte_usua_sin_afr, 2) OVER (
				PARTITION BY id_master ORDER BY num_periodo_sem
				) = 'USUARIO'
			THEN '5. Perdido Usuario'
		ELSE 'Sin Categoría 52s'
		END AS cod_estatus_52s_gen_sin_afr
        ,num_periodo_sem
FROM ${esquema_cu}.cu_esb_con_cte_act_sem_core
;

-- Inserted 23,521,640 row(s) 33s
COMPUTE STATS ${esquema_cu}.cu_esb_con_cte_actividad_sem;

--|************************************| PASO 05 |************************************|--
INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_sem 

SELECT *
FROM ${esquema_cu}.cu_esb_con_cte_actividad_sem
WHERE num_periodo_sem = ${num_periodo_sem} 
;

COMPUTE STATS ${esquema_cu}.cu_con_cte_actividad_sem;

--|************************************| PASO 06 TEMPORAL SOLO PARA EL REPROCESO |************************************|--
/**
INSERT OVERWRITE TABLE ws_ec_cd_baz_bdclientes.cd_con_cte_actividad_sem 
PARTITION(num_periodo_sem)

SELECT 
    id_master,
    ind_activo_cap,
    ind_activo_capnom,
    ind_activo_credimax,
    ind_activo_nom,
    ind_activo_tor,
    ind_activo_cre,
    ind_activo_rem,
    ind_activo_dex,
    ind_activo_div,
    ind_activo_pre,
    ind_activo_pgs,
    ind_activo_afr,
    ind_perdido_cap,
    ind_perdido_capnom,
    ind_perdido_credimax,
    ind_perdido_nom,
    ind_perdido_tor,
    ind_perdido_cre,
    ind_perdido_rem,
    ind_perdido_dex,
    ind_perdido_div,
    ind_perdido_pre,
    ind_perdido_pgs,
    ind_perdido_afr,
    num_sem_sld_0_credimax,
    num_sem_sld_0_nom,
    num_sem_sld_0_tor,
    num_sem_sld_0_cre,
    num_sem_inact_rem,
    num_sem_inact_dex,
    num_sem_inact_div,
    num_sem_inact_pre,
    num_sem_inact_pgs,
    num_sem_inact_afr,
    ind_inactivo_cap,
    ind_inactivo_capnom,
    ind_inactivo_credimax,
    ind_inactivo_nom,
    ind_inactivo_tor,
    ind_inactivo_cre,
    ind_inactivo_rem,
    ind_inactivo_dex,
    ind_inactivo_div,
    ind_inactivo_pre,
    ind_inactivo_pgs,
    ind_inactivo_afr,
    cod_estatus_semanal_cap,
    cod_estatus_semanal_capnom,
    cod_estatus_semanal_credimax,
    cod_estatus_semanal_nom,
    cod_estatus_semanal_tor,
    cod_estatus_semanal_cre,
    cod_estatus_semanal_rem,
    cod_estatus_semanal_dex,
    cod_estatus_semanal_div,
    cod_estatus_semanal_pre,
    cod_estatus_semanal_pgs,
    cod_estatus_semanal_afr,
    cod_estatus_52s_cap,
    cod_estatus_52s_capnom,
    cod_estatus_52s_credimax,
    cod_estatus_52s_nom,
    cod_estatus_52s_tor,
    cod_estatus_52s_cre,
    cod_estatus_52s_rem,
    cod_estatus_52s_dex,
    cod_estatus_52s_div,
    cod_estatus_52s_pre,
    cod_estatus_52s_pgs,
    cod_estatus_52s_afr,
    ind_activo,
    ind_activo_ctes_usua,
    ind_activo_solo_ctes,
    ind_activo_solo_usua,
    ind_activo_sin_afr,
    ind_activo_ctes_usua_sin_afr,
    ind_activo_solo_ctes_sin_afr,
    ind_activo_solo_usua_sin_afr,
    ind_inactivo,
    ind_inactivo_sin_afr,
    ind_perdido,
    ind_perdido_sin_afr,
    cod_cte_usua,
    cod_cte_usua_sin_afr,
    cod_estatus_semanal_gen,
    cod_estatus_52s_gen,
    cod_estatus_semanal_gen_sin_afr,
    cod_estatus_52s_gen_sin_afr,
    NULL AS id_md5,
    NULL AS id_md5completo,
    "2022-04-29 15:22:38.097433000" AS fec_carga,
    num_periodo_sem
FROM ${esquema_cu}.cu_con_cte_actividad_sem
;

COMPUTE INCREMENTAL STATS ws_ec_cd_baz_bdclientes.cd_con_cte_actividad_sem ;
**/