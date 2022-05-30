-- Proceso de Actividad v2.1
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>
-- General

-- Creamos la tabla con el estatus de actividad/perdidos general
-- Unimos las tablas de los diferentes negocios para obetenr la tabla final de actividad

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_prev_sem
PARTITION (num_periodo_sem = ${num_periodo_sem})

WITH 
	PIVOTE AS ( -- Obtenemos un pivote de clientes para evitar algunos casos de duplicados (que no deberían pasar)

SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_cap_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_capnom_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_cre_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_tor_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_rem_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_dex_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_div_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_pre_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_pgs_sem WHERE num_periodo_sem = ${num_periodo_sem}
UNION
SELECT id_master
FROM ${esquema_cd}.cd_con_cte_actividad_afr_sem WHERE num_periodo_sem = ${num_periodo_sem}
)
, ACT AS  ( -- Obtenemos las marcas de actividad/inactividad/perdidos y cliente/usuario
    SELECT pvt.id_master
          -- Marcas de Actividad
          ,COALESCE(cap.ind_activo_cap, 0) AS ind_activo_cap
          ,COALESCE(capnom.ind_activo_capnom, 0) AS ind_activo_capnom
          ,COALESCE(cre.ind_activo_credimax, 0) AS ind_activo_credimax
          ,COALESCE(cre.ind_activo_nom, 0) AS ind_activo_nom
          ,COALESCE(cre.ind_activo_tor, 0) AS ind_activo_tor
          ,COALESCE(cre.ind_activo_cre, 0) AS ind_activo_cre
          ,COALESCE(rem.ind_activo_rem, 0) AS ind_activo_rem
          ,COALESCE(dex.ind_activo_dex, 0) AS ind_activo_dex
          ,COALESCE(dvs.ind_activo_div, 0) AS ind_activo_div
          ,COALESCE(pre.ind_activo_pre, 0) AS ind_activo_pre
          ,COALESCE(pgs.ind_activo_pgs, 0) AS ind_activo_pgs
          ,COALESCE(afr.ind_activo_afr, 0) AS ind_activo_afr
          -- Marcas de Perdidos
          ,COALESCE(cap.ind_perdido_cap, 0) AS ind_perdido_cap
          ,COALESCE(capnom.ind_perdido_capnom, 0) AS ind_perdido_capnom
          ,COALESCE(cre.ind_perdido_credimax, 0) AS ind_perdido_credimax
          ,COALESCE(cre.ind_perdido_nom, 0) AS ind_perdido_nom
          ,COALESCE(cre.ind_perdido_tor, 0) AS ind_perdido_tor
          ,COALESCE(cre.ind_perdido_cre, 0) AS ind_perdido_cre
          ,COALESCE(rem.ind_perdido_rem, 0) AS ind_perdido_rem
          ,COALESCE(dex.ind_perdido_dex, 0) AS ind_perdido_dex
          ,COALESCE(dvs.ind_perdido_div, 0) AS ind_perdido_div
          ,COALESCE(pre.ind_perdido_pre, 0) AS ind_perdido_pre
          ,COALESCE(pgs.ind_perdido_pgs, 0) AS ind_perdido_pgs
          ,COALESCE(afr.ind_perdido_afr, 0) AS ind_perdido_afr
          -- Semanas de inactividad por negocio (ojo con crédito general, no es directa la relación entre meses de inactividad e inactivo)
          
		  ,COALESCE(cre.num_sem_sld_0_credimax, 0) AS num_sem_sld_0_credimax
          ,COALESCE(cre.num_sem_sld_0_nom, 0) AS num_sem_sld_0_nom
          ,COALESCE(cre.num_sem_sld_0_tor, 0) AS num_sem_sld_0_tor
          ,COALESCE(cre.num_sem_sld_0_cre, 0) AS num_sem_sld_0_cre
          ,COALESCE(rem.num_sem_inact_rem, 0) AS num_sem_inact_rem
          ,COALESCE(dex.num_sem_inact_dex, 0) AS num_sem_inact_dex
          ,COALESCE(dvs.num_sem_inact_div, 0) AS num_sem_inact_div
          ,COALESCE(pre.num_sem_inact_pre, 0) AS num_sem_inact_pre
          ,COALESCE(pgs.num_sem_inact_pgs, 0) AS num_sem_inact_pgs
          ,COALESCE(afr.num_sem_inact_afr, 0) AS num_sem_inact_afr
          -- Indicadoras de Inactividad
		  ,COALESCE(cap.ind_inactivo_cap, 0) AS ind_inactivo_cap ---
		  ,COALESCE(capnom.ind_inactivo_capnom, 0) AS ind_inactivo_capnom ---
          ,COALESCE(cre.ind_inactivo_credimax, 0) AS ind_inactivo_credimax
          ,COALESCE(cre.ind_inactivo_nom, 0) AS ind_inactivo_nom
          ,COALESCE(cre.ind_inactivo_tor, 0) AS ind_inactivo_tor
          ,COALESCE(cre.ind_inactivo_cre, 0) AS ind_inactivo_cre
          ,IF(COALESCE(rem.num_sem_inact_rem, 0) BETWEEN 1 AND 39, 1, 0) AS ind_inactivo_rem
          ,IF(COALESCE(dex.num_sem_inact_dex, 0) BETWEEN 1 AND 39, 1, 0) AS ind_inactivo_dex
          ,IF(COALESCE(dvs.num_sem_inact_div, 0) BETWEEN 1 AND 39, 1, 0) AS ind_inactivo_div
          ,IF(COALESCE(pre.num_sem_inact_pre, 0) BETWEEN 1 AND 8, 1, 0) AS ind_inactivo_pre
          ,IF(COALESCE(pgs.num_sem_inact_pgs, 0) BETWEEN 1 AND 13, 1, 0) AS ind_inactivo_pgs
          ,IF(COALESCE(afr.num_sem_inact_afr, 0) BETWEEN 1 AND 39, 1, 0) AS ind_inactivo_afr -- Dummie de Afore, no sirve de nada por ahora (Hay que hacer el cambio cuando tengamos información)
          -- Codigos de estatus mensual por negocio
         ,COALESCE(cap.cod_estatus_semanal_cap, 'Sin Categoría Semanal') AS cod_estatus_semanal_cap
         ,COALESCE(capnom.cod_estatus_semanal_capnom, 'Sin Categoría Semanal') AS cod_estatus_semanal_capnom
         ,COALESCE(cre.cod_estatus_semanal_credimax, 'Sin Categoría Semanal') AS cod_estatus_semanal_credimax
         ,COALESCE(cre.cod_estatus_semanal_nom, 'Sin Categoría Semanal') AS cod_estatus_semanal_nom
         ,COALESCE(cre.cod_estatus_semanal_tor, 'Sin Categoría Semanal') AS cod_estatus_semanal_tor
         ,COALESCE(cre.cod_estatus_semanal_cre, 'Sin Categoría Semanal') AS cod_estatus_semanal_cre
         ,COALESCE(rem.cod_estatus_semanal_rem, 'Sin Categoría Semanal') AS cod_estatus_semanal_rem
         ,COALESCE(dex.cod_estatus_semanal_dex, 'Sin Categoría Semanal') AS cod_estatus_semanal_dex
         ,COALESCE(dvs.cod_estatus_semanal_div, 'Sin Categoría Semanal') AS cod_estatus_semanal_div
         ,COALESCE(pre.cod_estatus_semanal_pre, 'Sin Categoría Semanal') AS cod_estatus_semanal_pre
         ,COALESCE(pgs.cod_estatus_semanal_pgs, 'Sin Categoría Semanal') AS cod_estatus_semanal_pgs
         ,COALESCE(afr.cod_estatus_semanal_afr, 'Sin Categoría Semanal') AS cod_estatus_semanal_afr
         -- Codigos de estatus anual por negocio
         ,COALESCE(cap.cod_estatus_52s_cap, 'Sin Categoría 52sem') AS cod_estatus_52s_cap
         ,COALESCE(capnom.cod_estatus_52s_capnom, 'Sin Categoría 52sem') AS cod_estatus_52s_capnom
         ,COALESCE(cre.cod_estatus_52s_credimax, 'Sin Categoría 52sem') AS cod_estatus_52s_credimax
         ,COALESCE(cre.cod_estatus_52s_nom, 'Sin Categoría 52sem') AS cod_estatus_52s_nom
         ,COALESCE(cre.cod_estatus_52s_tor, 'Sin Categoría 52sem') AS cod_estatus_52s_tor
         ,COALESCE(cre.cod_estatus_52s_cre, 'Sin Categoría 52sem') AS cod_estatus_52s_cre
         ,COALESCE(rem.cod_estatus_52s_rem, 'Sin Categoría 52sem') AS cod_estatus_52s_rem
         ,COALESCE(dex.cod_estatus_52s_dex, 'Sin Categoría 52sem') AS cod_estatus_52s_dex
         ,COALESCE(dvs.cod_estatus_52s_div, 'Sin Categoría 52sem') AS cod_estatus_52s_div
         ,COALESCE(pre.cod_estatus_52s_pre, 'Sin Categoría 52sem') AS cod_estatus_52s_pre
         ,COALESCE(pgs.cod_estatus_52s_pgs, 'Sin Categoría 52sem') AS cod_estatus_52s_pgs
         ,COALESCE(afr.cod_estatus_52s_afr, 'Sin Categoría 52sem') AS cod_estatus_52s_afr
         -- Indicadoras de Actividad Cliente/Usuario con Afore
         ,GREATEST(COALESCE(cap.ind_activo_cap, 0)
                  ,COALESCE(capnom.ind_activo_capnom, 0)
                  ,COALESCE(cre.ind_activo_cre, 0)
                  ,COALESCE(pre.ind_activo_pre, 0)
                  ,COALESCE(rem.ind_activo_rem, 0)
                  ,COALESCE(dex.ind_activo_dex, 0)
                  ,COALESCE(dvs.ind_activo_div, 0)
                  ,COALESCE(pgs.ind_activo_pgs, 0)
                  ,COALESCE(afr.ind_activo_afr, 0)) AS ind_activo
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(afr.ind_activo_afr, 0)) = 1 AND
                    GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 1 THEN 1
          ELSE 0 END AS ind_activo_ctes_usua
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(afr.ind_activo_afr, 0)) = 1 AND
                    GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 0 THEN 1
          ELSE 0 END AS ind_activo_solo_ctes
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(afr.ind_activo_afr, 0)) = 0 AND
                    GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 1 THEN 1
          ELSE 0 END AS ind_activo_solo_usua
          -- Indicadoras de Actividad Cliente/Usuario sin Afore
         ,GREATEST(COALESCE(cap.ind_activo_cap, 0)
                  ,COALESCE(capnom.ind_activo_capnom, 0)
                  ,COALESCE(cre.ind_activo_cre, 0)
                  ,COALESCE(pre.ind_activo_pre, 0)
                  ,COALESCE(rem.ind_activo_rem, 0)
                  ,COALESCE(dex.ind_activo_dex, 0)
                  ,COALESCE(dvs.ind_activo_div, 0)
                  ,COALESCE(pgs.ind_activo_pgs, 0)) AS ind_activo_sin_afr
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)) = 1 AND
                    GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 1 THEN 1
          ELSE 0 END AS ind_activo_ctes_usua_sin_afr
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)) = 1 AND
                    GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 0 THEN 1
          ELSE 0 END AS ind_activo_solo_ctes_sin_afr
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)) = 0 AND
                    GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 1 THEN 1
          ELSE 0 END AS ind_activo_solo_usua_sin_afr
          -- Indicadora de Inactivo general con Afore
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)
                            ,COALESCE(afr.ind_activo_afr, 0)) = 0 AND
                    GREATEST(COALESCE(cap.ind_inactivo_cap, 0)
							,COALESCE(capnom.ind_inactivo_capnom, 0)
							,COALESCE(cre.ind_inactivo_cre, 0)
                            ,IF(COALESCE(pre.num_sem_inact_pre, 0) BETWEEN 1 AND 8, 1, 0)
                            ,IF(COALESCE(rem.num_sem_inact_rem, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dex.num_sem_inact_dex, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dvs.num_sem_inact_div, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(pgs.num_sem_inact_pgs, 0) BETWEEN 1 AND 13, 1, 0)) = 1 THEN 1
          ELSE 0 END AS ind_inactivo
         -- Indicadora de Inactivo general sin Afore
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 0 AND
                    GREATEST(COALESCE(cap.ind_inactivo_cap, 0)
							,COALESCE(capnom.ind_inactivo_capnom, 0)
							,COALESCE(cre.ind_inactivo_cre, 0)
                            ,IF(COALESCE(pre.num_sem_inact_pre, 0) BETWEEN 1 AND 8, 1, 0)
                            ,IF(COALESCE(rem.num_sem_inact_rem, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dex.num_sem_inact_dex, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dvs.num_sem_inact_div, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(pgs.num_sem_inact_pgs, 0) BETWEEN 1 AND 13, 1, 0)) = 1 THEN 1
          ELSE 0 END AS ind_inactivo_sin_afr
          -- Indicadoras de Perdido general con Afore
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)
                            ,COALESCE(afr.ind_activo_afr, 0)) = 0 AND
                    GREATEST(COALESCE(cap.ind_inactivo_cap, 0)
							,COALESCE(capnom.ind_inactivo_capnom, 0)
							,COALESCE(cre.ind_inactivo_cre, 0)
                            ,IF(COALESCE(pre.num_sem_inact_pre, 0) BETWEEN 1 AND 8, 1, 0)
                            ,IF(COALESCE(rem.num_sem_inact_rem, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dex.num_sem_inact_dex, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dvs.num_sem_inact_div, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(pgs.num_sem_inact_pgs, 0) BETWEEN 1 AND 13, 1, 0)) = 0 AND
                    GREATEST(COALESCE(cap.ind_perdido_cap, 0)
                            ,COALESCE(capnom.ind_perdido_capnom, 0)
                            ,COALESCE(cre.ind_perdido_cre, 0)
                            ,COALESCE(rem.ind_perdido_rem, 0)
                            ,COALESCE(dex.ind_perdido_dex, 0)
                            ,COALESCE(dvs.ind_perdido_div, 0)
                            ,COALESCE(pre.ind_perdido_pre, 0)
                            ,COALESCE(pgs.ind_perdido_pgs, 0)
                            ,COALESCE(afr.ind_perdido_afr, 0)) = 1 THEN 1
              ELSE 0 END AS ind_perdido
          -- Indicadora de Perdido general sin Afore
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 0 AND
                    GREATEST(COALESCE(cap.ind_inactivo_cap, 0)
							,COALESCE(capnom.ind_inactivo_capnom, 0)
							,COALESCE(cre.ind_inactivo_cre, 0)
                            ,IF(COALESCE(pre.num_sem_inact_pre, 0) BETWEEN 1 AND 8, 1, 0)
                            ,IF(COALESCE(rem.num_sem_inact_rem, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dex.num_sem_inact_dex, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(dvs.num_sem_inact_div, 0) BETWEEN 1 AND 39, 1, 0)
                            ,IF(COALESCE(pgs.num_sem_inact_pgs, 0) BETWEEN 1 AND 13, 1, 0)) = 0 AND
                    GREATEST(COALESCE(cap.ind_perdido_cap, 0)
                            ,COALESCE(capnom.ind_perdido_capnom, 0)
                            ,COALESCE(cre.ind_perdido_cre, 0)
                            ,COALESCE(rem.ind_perdido_rem, 0)
                            ,COALESCE(dex.ind_perdido_dex, 0)
                            ,COALESCE(dvs.ind_perdido_div, 0)
                            ,COALESCE(pre.ind_perdido_pre, 0)
                            ,COALESCE(pgs.ind_perdido_pgs, 0)) = 1 THEN 1
          ELSE 0 END AS ind_perdido_sin_afr
          -- Bandera de CLIENTE/USUARIO con Afore
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)
                            ,COALESCE(afr.ind_activo_afr, 0)) = 1 THEN 'CLIENTE'
               WHEN GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 1 THEN 'USUARIO'
               WHEN (COALESCE(cap.ind_inactivo_cap, 0) = 1 OR ---  
					 COALESCE(capnom.ind_inactivo_capnom, 0) = 1 OR ---
					 COALESCE(cre.ind_inactivo_cre, 0) = 1 OR
                     COALESCE(pre.num_sem_inact_pre, 0) BETWEEN 1 AND 8 OR
                     COALESCE(afr.num_sem_inact_afr, 0) BETWEEN 1 AND 39) THEN 'CLIENTE'
               WHEN (COALESCE(num_sem_inact_rem, 0) BETWEEN 1 AND 39) OR
                    (COALESCE(num_sem_inact_dex, 0) BETWEEN 1 AND 39) OR
                    (COALESCE(num_sem_inact_div, 0) BETWEEN 1 AND 39) OR
                    (COALESCE(num_sem_inact_pgs, 0) BETWEEN 1 AND 13) THEN 'USUARIO'
               WHEN GREATEST(COALESCE(ind_perdido_cap, 0)
                            ,COALESCE(ind_perdido_capnom, 0)
                            ,COALESCE(ind_perdido_cre, 0)
                            ,COALESCE(ind_perdido_pre, 0)
                            ,COALESCE(ind_perdido_afr, 0)) = 1 THEN 'CLIENTE'
               WHEN GREATEST(COALESCE(ind_perdido_rem, 0)
                            ,COALESCE(ind_perdido_dex, 0)
                            ,COALESCE(ind_perdido_div, 0)
                            ,COALESCE(ind_perdido_pgs, 0)) = 1 THEN 'USUARIO'
               ELSE 'OTRO' END AS cod_cte_usua
          -- Bandera de CLIENTE/USUARIO sin Afore
         ,CASE WHEN GREATEST(COALESCE(cap.ind_activo_cap, 0)
                            ,COALESCE(capnom.ind_activo_capnom, 0)
                            ,COALESCE(cre.ind_activo_cre, 0)
                            ,COALESCE(pre.ind_activo_pre, 0)) = 1 THEN 'CLIENTE'
               WHEN GREATEST(COALESCE(rem.ind_activo_rem, 0)
                            ,COALESCE(dex.ind_activo_dex, 0)
                            ,COALESCE(dvs.ind_activo_div, 0)
                            ,COALESCE(pgs.ind_activo_pgs, 0)) = 1 THEN 'USUARIO'
               WHEN (COALESCE(cap.ind_inactivo_cap, 0) = 1 OR ---  
					 COALESCE(capnom.ind_inactivo_capnom, 0) = 1 OR ---
					 COALESCE(cre.ind_inactivo_cre, 0) = 1 OR
                     COALESCE(pre.num_sem_inact_pre, 0) BETWEEN 1 AND 8) THEN 'CLIENTE'
               WHEN (COALESCE(num_sem_inact_rem, 0) BETWEEN 1 AND 39) OR
                    (COALESCE(num_sem_inact_dex, 0) BETWEEN 1 AND 39) OR
                    (COALESCE(num_sem_inact_div, 0) BETWEEN 1 AND 39) OR
                    (COALESCE(num_sem_inact_pgs, 0) BETWEEN 1 AND 13) THEN 'USUARIO'
               WHEN GREATEST(COALESCE(ind_perdido_cap, 0)
                            ,COALESCE(ind_perdido_capnom, 0)
                            ,COALESCE(ind_perdido_cre, 0)
                            ,COALESCE(ind_perdido_pre, 0)) = 1 THEN 'CLIENTE'
               WHEN GREATEST(COALESCE(ind_perdido_rem, 0)
                            ,COALESCE(ind_perdido_dex, 0)
                            ,COALESCE(ind_perdido_div, 0)
                            ,COALESCE(ind_perdido_pgs, 0)) = 1 THEN 'USUARIO'
               ELSE 'OTRO' END AS cod_cte_usua_sin_afr
    FROM PIVOTE AS pvt

    LEFT JOIN -- Obtenemos las marcas de Captación
(SELECT id_master, ind_activo_cap, ind_inactivo_cap, ind_perdido_cap, cod_estatus_semanal_cap, cod_estatus_52s_cap
 FROM ${esquema_cd}.cd_con_cte_actividad_cap_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS cap
ON pvt.id_master = cap.id_master

LEFT JOIN -- Obtenemos las marcas de Captación Nómina
(SELECT id_master, ind_activo_capnom, ind_inactivo_capnom, ind_perdido_capnom, cod_estatus_semanal_capnom, cod_estatus_52s_capnom
 FROM ${esquema_cd}.cd_con_cte_actividad_capnom_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS capnom
ON pvt.id_master = capnom.id_master

LEFT JOIN -- Obtenemos las marcas de Crédito
(SELECT id_master, ind_activo_credimax, ind_activo_nom, ind_activo_tor, ind_activo_cre
       ,ind_inactivo_credimax, ind_inactivo_nom, ind_inactivo_tor, ind_inactivo_cre
       ,ind_perdido_credimax, ind_perdido_nom, ind_perdido_tor, ind_perdido_cre
       ,num_sem_sld_0_credimax, num_sem_sld_0_nom, num_sem_sld_0_tor, num_sem_sld_0_cre
       ,cod_estatus_semanal_credimax, cod_estatus_semanal_nom, cod_estatus_semanal_tor, cod_estatus_semanal_cre
       ,cod_estatus_52s_credimax, cod_estatus_52s_nom, cod_estatus_52s_tor, cod_estatus_52s_cre
 FROM ${esquema_cd}.cd_con_cte_actividad_cre_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS cre
ON pvt.id_master = cre.id_master

LEFT JOIN -- Obtenemos las marcas de Remesas
(SELECT id_master ,ind_activo_rem, ind_perdido_rem, num_sem_inact AS num_sem_inact_rem, cod_estatus_semanal_rem, cod_estatus_52s_rem
 FROM ${esquema_cd}.cd_con_cte_actividad_rem_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS rem
ON pvt.id_master = rem.id_master

LEFT JOIN -- Obtenemos las marcas de DEX
(SELECT id_master ,ind_activo_dex, ind_perdido_dex, num_sem_inact AS num_sem_inact_dex, cod_estatus_semanal_dex, cod_estatus_52s_dex
 FROM ${esquema_cd}.cd_con_cte_actividad_dex_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS dex
ON pvt.id_master = dex.id_master

LEFT JOIN -- Obtenemos las marcas de Divisas
(SELECT id_master, ind_activo_div, ind_perdido_div, num_sem_inact AS num_sem_inact_div, cod_estatus_semanal_div, cod_estatus_52s_div
 FROM ${esquema_cd}.cd_con_cte_actividad_div_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS dvs
ON pvt.id_master = dvs.id_master

LEFT JOIN -- Obtenemos las marcas de Prendario
(SELECT id_master, ind_activo_pre, ind_perdido_pre, num_sem_inact AS num_sem_inact_pre, cod_estatus_semanal_pre, cod_estatus_52s_pre
 FROM ${esquema_cd}.cd_con_cte_actividad_pre_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS pre
ON pvt.id_master = pre.id_master

LEFT JOIN -- Obtenemos las marcas de PGS
(SELECT id_master, ind_activo_pgs, ind_perdido_pgs, num_sem_inact AS num_sem_inact_pgs, cod_estatus_semanal_pgs, cod_estatus_52s_pgs
 FROM ${esquema_cd}.cd_con_cte_actividad_pgs_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS pgs
ON pvt.id_master = pgs.id_master

LEFT JOIN -- Obtenemos las marcas de Afore
(SELECT id_master, ind_activo_afr, ind_perdido_afr, num_sem_inact AS num_sem_inact_afr, cod_estatus_semanal_afr, cod_estatus_52s_afr
 FROM ${esquema_cd}.cd_con_cte_actividad_afr_sem WHERE num_periodo_sem = ${num_periodo_sem}) AS afr
ON pvt.id_master = afr.id_master

)
SELECT id_master
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
      ,ind_inactivo_cap --- 
	  ,ind_inactivo_capnom ---
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
FROM ACT
;

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_con_cte_actividad_prev_sem;

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_pivot_aux
select id_master
      ,MIN(IF(ind_activo = 1, num_periodo_sem, NULL)) AS min_sem_act
      ,MIN(IF(ind_activo_sin_afr = 1, num_periodo_sem, NULL)) AS min_sem_act_sin_afr
from ${esquema_cu}.cu_con_cte_actividad_prev_sem
where ind_activo = 1 or ind_activo_sin_afr = 1
group by 1
;

COMPUTE STATS ${esquema_cu}.cu_con_cte_actividad_pivot_aux;