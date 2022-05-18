-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Crédito General
-- Creamos la tabla general de crédito
-- Unimos las tablas de los diferentes sub-negocios de crédito para generar la vista general

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_cre_sem
PARTITION(num_periodo_sem)

WITH PIVOTE AS ( -- Obtenemos el pivote de clientes y semanas necesarias para la vista semanal y 52 semanas
    SELECT master.id_master, semanas.num_periodo_sem
    FROM

     (SELECT DISTINCT id_master FROM ${esquema_cu}.cu_con_cte_actividad_credimax_sem
      WHERE num_periodo_sem = ${num_periodo_sem}
      UNION
      SELECT DISTINCT id_master FROM ${esquema_cu}.cu_con_cte_actividad_nom_sem
      WHERE num_periodo_sem = ${num_periodo_sem}
      UNION
      SELECT DISTINCT id_master FROM ${esquema_cu}.cu_con_cte_actividad_tor_sem
      WHERE num_periodo_sem = ${num_periodo_sem}) AS master

    CROSS JOIN
    (SELECT DISTINCT num_periodo_sem
     FROM cd_baz_bdclientes.cd_gen_fechas_cat
     WHERE num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS semanas

)
, PREV_ACT AS (
SELECT pivote.id_master
      ,pivote.num_periodo_sem
      -- Indicadoras de Actividad
      ,COALESCE(credimax.ind_activo_credimax, 0) AS ind_activo_credimax
      ,COALESCE(nom.ind_activo_nom, 0) AS ind_activo_nom
      ,COALESCE(tor.ind_activo_tor, 0) AS ind_activo_tor
      ,CASE WHEN COALESCE(credimax.ind_activo_credimax, 0) + COALESCE(nom.ind_activo_nom, 0) + COALESCE(tor.ind_activo_tor, 0) >= 1 THEN 1
       ELSE 0 END AS ind_activo_cre
       -- Indicadoras de Inactividad
      ,COALESCE(credimax.ind_inactivo_credimax, 0) AS ind_inactivo_credimax
      ,COALESCE(nom.ind_inactivo_nom, 0) AS ind_inactivo_nom
      ,COALESCE(tor.ind_inactivo_tor, 0) AS ind_inactivo_tor
      ,CASE WHEN COALESCE(credimax.ind_activo_credimax, 0) + COALESCE(nom.ind_activo_nom, 0) + COALESCE(tor.ind_activo_tor, 0) = 0 AND
                 COALESCE(credimax.ind_inactivo_credimax, 0) + COALESCE(nom.ind_inactivo_nom, 0) + COALESCE(tor.ind_inactivo_tor, 0) >= 1 THEN 1
       ELSE 0 END AS ind_inactivo_cre
       -- Indicadoras de Perdido
      ,COALESCE(credimax.ind_perdido_credimax, 0) AS ind_perdido_credimax
      ,COALESCE(nom.ind_perdido_nom, 0) AS ind_perdido_nom
      ,COALESCE(tor.ind_perdido_tor, 0) AS ind_perdido_tor
      ,CASE WHEN COALESCE(credimax.ind_activo_credimax, 0) + COALESCE(nom.ind_activo_nom, 0) + COALESCE(tor.ind_activo_tor, 0) = 0 AND
                 COALESCE(credimax.ind_inactivo_credimax, 0) + COALESCE(nom.ind_inactivo_nom, 0) + COALESCE(tor.ind_inactivo_tor, 0) = 0 AND
                 COALESCE(credimax.ind_perdido_credimax, 0) + COALESCE(nom.ind_perdido_nom, 0) + COALESCE(tor.ind_perdido_tor, 0) = 1 THEN 1
       ELSE 0 END AS ind_perdido_cre
       -- Semanas de atraso
      ,COALESCE(credimax.num_sem_atraso, 0) AS num_sem_atraso_credimax
      ,COALESCE(nom.num_sem_atraso, 0) AS num_sem_atraso_nom
      ,COALESCE(tor.num_sem_atraso, 0) AS num_sem_atraso_tor
      ,GREATEST(COALESCE(credimax.num_sem_atraso, 0), COALESCE(nom.num_sem_atraso, 0), COALESCE(tor.num_sem_atraso, 0)) AS num_sem_atraso_cre
      -- Semanas con saldo 0 (Para Crédito general no aplicaexactamente los semanas de slado $0 general)
      ,COALESCE(credimax.num_sem_sld_0, 0) AS num_sem_sld_0_credimax
      ,COALESCE(nom.num_sem_sld_0, 0) AS num_sem_sld_0_nom
      ,COALESCE(tor.num_sem_sld_0, 0) AS num_sem_sld_0_tor
      ,GREATEST(COALESCE(credimax.num_sem_sld_0, 0), COALESCE(nom.num_sem_sld_0, 0), COALESCE(tor.num_sem_sld_0, 0)) AS num_sem_sld_0_cre
      -- Categorías de Estatús semanal
      ,COALESCE(credimax.cod_estatus_semanal_credimax, 'Sin Categoría Semanal') AS cod_estatus_semanal_credimax
      ,COALESCE(nom.cod_estatus_semanal_nom, 'Sin Categoría Semanal') AS cod_estatus_semanal_nom
      ,COALESCE(tor.cod_estatus_semanal_tor, 'Sin Categoría Semanal') AS cod_estatus_semanal_tor
      -- Categorías de Estátus 52 semanas
      ,COALESCE(credimax.cod_estatus_52s_credimax, 'Sin Categoría 52sem') AS cod_estatus_52s_credimax
      ,COALESCE(nom.cod_estatus_52s_nom, 'Sin Categoría 52sem') AS cod_estatus_52s_nom
      ,COALESCE(tor.cod_estatus_52s_tor, 'Sin Categoría 52sem') AS cod_estatus_52s_tor
FROM PIVOTE
LEFT JOIN ${esquema_cu}.cu_con_cte_actividad_credimax_sem AS credimax
ON pivote.id_master = credimax.id_master AND
   pivote.num_periodo_sem = credimax.num_periodo_sem
LEFT JOIN ${esquema_cu}.cu_con_cte_actividad_nom_sem AS nom
ON pivote.id_master = nom.id_master AND
   pivote.num_periodo_sem = nom.num_periodo_sem
LEFT JOIN ${esquema_cu}.cu_con_cte_actividad_tor_sem AS tor
ON pivote.id_master = tor.id_master AND
   pivote.num_periodo_sem = tor.num_periodo_sem
)
, ACT AS (
SELECT *
      -- Categorías de Estatús semanal generales
      ,CASE WHEN COALESCE(ind_activo_cre, 0) = 1 AND
                 COALESCE(LAG(ind_activo_cre, 1) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 THEN '1. Se Mantiene Activo'
            WHEN COALESCE(ind_activo_cre, 0) = 1 AND
                 COALESCE(LAG(ind_activo_cre, 1) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 AND
                 IF(COALESCE(cod_estatus_semanal_credimax, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_semanal_tor, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_semanal_nom, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) = 0 THEN '2. Nuevo'
            WHEN COALESCE(ind_activo_cre, 0) = 1 AND
                 COALESCE(LAG(ind_activo_cre, 1) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 AND
                 IF(COALESCE(cod_estatus_semanal_credimax, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_semanal_tor, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_semanal_nom, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) >= 1 THEN '3. Reactivado'
            WHEN COALESCE(ind_activo_cre, 0) = 0 AND COALESCE(ind_perdido_cre, 0) = 0 AND
                 COALESCE(LAG(ind_activo_cre, 1) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 THEN '4. Inactivado'
                 -- IF(COALESCE(cod_estatus_semanal_credimax, 'Sin Categoría Semanal') IN ('5. Perdido'), 1, 0) +
                 -- IF(COALESCE(cod_estatus_semanal_tor, 'Sin Categoría Semanal') IN ('5. Perdido'), 1, 0) +
                 -- IF(COALESCE(cod_estatus_semanal_nom, 'Sin Categoría Semanal') IN ('5. Perdido'), 1, 0) = 0 THEN '4. Inactivado'
            WHEN COALESCE(ind_perdido_cre, 0) = 1 AND
                 COALESCE(LAG(ind_activo_cre, 1) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 THEN '5. Perdido'
            ELSE 'Sin Categoría Semanal' END AS cod_estatus_semanal_cre
      -- Categorías de Estatús 52s generales
      ,CASE WHEN COALESCE(ind_activo_cre, 0) = 1 AND
                 COALESCE(LAG(ind_activo_cre, 52) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 THEN '1. Se Mantiene Activo'
            WHEN COALESCE(ind_activo_cre, 0) = 1 AND
                 COALESCE(LAG(ind_activo_cre, 52) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 AND
                 IF(COALESCE(cod_estatus_52s_credimax, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_tor, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_nom, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) = 0 THEN '2. Nuevo'
            WHEN COALESCE(ind_activo_cre, 0) = 1 AND
                 COALESCE(LAG(ind_activo_cre, 52) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 0 AND
                 IF(COALESCE(cod_estatus_52s_credimax, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_tor, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_nom, 'Sin Categoría Semanal') IN ('3. Reactivado'), 1, 0) >= 1 THEN '3. Reactivado'
            WHEN COALESCE(ind_activo_cre, 0) = 0 AND
                 COALESCE(LAG(ind_activo_cre, 52) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 AND
                 IF(COALESCE(cod_estatus_52s_credimax, 'Sin Categoría 52sem') IN ('5. Perdido'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_tor, 'Sin Categoría 52sem') IN ('5. Perdido'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_nom, 'Sin Categoría 52sem') IN ('5. Perdido'), 1, 0) = 0 THEN '4. Inactivado'
            WHEN COALESCE(ind_activo_cre, 0) = 0 AND
                 COALESCE(LAG(ind_activo_cre, 52) OVER(PARTITION BY id_master ORDER BY num_periodo_sem), 0) = 1 AND
                 IF(COALESCE(cod_estatus_52s_credimax, 'Sin Categoría 52sem') IN ('5. Perdido'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_tor, 'Sin Categoría 52sem') IN ('5. Perdido'), 1, 0) +
                 IF(COALESCE(cod_estatus_52s_nom, 'Sin Categoría 52sem') IN ('5. Perdido'), 1, 0) >= 1 THEN '5. Perdido'
            ELSE 'Sin Categoría 52sem' END AS cod_estatus_52s_cre
FROM PREV_ACT AS pvt
)
SELECT
	id_master,
	ind_activo_credimax,
	ind_activo_nom,
	ind_activo_tor,
	ind_activo_cre,
	ind_inactivo_credimax,
	ind_inactivo_nom,
	ind_inactivo_tor,
	ind_inactivo_cre,
	ind_perdido_credimax,
	ind_perdido_nom,
	ind_perdido_tor,
	ind_perdido_cre,
	num_sem_atraso_credimax,
	num_sem_atraso_nom,
	num_sem_atraso_tor,
	num_sem_atraso_cre,
	num_sem_sld_0_credimax,
	num_sem_sld_0_nom,
	num_sem_sld_0_tor,
	num_sem_sld_0_cre,
	cod_estatus_semanal_credimax,
	cod_estatus_semanal_nom,
	cod_estatus_semanal_tor,
	cod_estatus_52s_credimax,
	cod_estatus_52s_nom,
	cod_estatus_52s_tor,
	cod_estatus_semanal_cre,
	cod_estatus_52s_cre,
	num_periodo_sem
FROM ACT
WHERE num_periodo_sem = ${num_periodo_sem};