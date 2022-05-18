-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Credimax
-- Creamos la tabla con el estatus de actividad/perdidos de Credimax
-- Necesitamos tener la vista de 12 meses antes del cliente para obtener la bandera de vista anual

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_credimax_sem
PARTITION(num_periodo_sem)
WITH 
PIVOTE AS (
    SELECT credimax.id_master, semanas.num_periodo_sem, semanas.id
    FROM
        (SELECT DISTINCT id_master
        FROM ${esquema_cu}.cu_con_cte_actividad_credimax_aux_sem
        WHERE sld_tot_pendiente_credimax > 0
        AND num_sem_atraso_credimax <= 39
        AND num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}) AS credimax

    CROSS JOIN
    (SELECT num_periodo_sem, id
    FROM (SELECT NUM_PERIODO_SEM
                 ,ROW_NUMBER() OVER (ORDER BY NUM_PERIODO_SEM) ID
          FROM cd_baz_bdclientes.cd_gen_fechas_cat
          GROUP BY NUM_PERIODO_SEM) fec_cat
    WHERE num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}) AS semanas
),
PREVIA_ESTATUS AS (
     -- Calculamos las semanas que ha tenido saldo = $0 desde la última vez que tuvo saldo > $0, y obtenemos la antigüedad
    SELECT x.id_master
            ,x.num_periodo_sem
            ,x.sld_tot_pendiente
            ,x.num_sem_atraso
            ,x.ind_sld
            ,x.id - fec_cat.id as num_sem_sld_0 
            ,x.fec_ant_credimax
    FROM (SELECT pvt.id_master, pvt.num_periodo_sem, pvt.id
                ,COALESCE(credimax.sld_tot_pendiente_credimax, 0) AS sld_tot_pendiente
                ,COALESCE(credimax.num_sem_atraso_credimax, 0) AS num_sem_atraso
                ,COALESCE(credimax.ind_sld, 0) AS ind_sld
                ,MAX(pvt.num_periodo_sem*COALESCE(credimax.ind_sld,0)) OVER(PARTITION BY pvt.id_master) AS max_periodo_sem
                ,COALESCE(
                      CASE
                        WHEN MONTH(fec_cre) = 1 AND WEEKOFYEAR(fec_cre) >= 50 -- DIC/ENE
                            THEN CAST(CONCAT(CAST(YEAR(fec_cre) - 1 AS STRING),LPAD(CAST(WEEKOFYEAR(fec_cre) AS STRING), 2, '0')) AS INT)
                        WHEN MONTH(fec_cre) = 12 AND WEEKOFYEAR(fec_cre) = 1 -- ENE/DIC
                            THEN CAST(CONCAT(CAST(YEAR(fec_cre) + 1 AS STRING),LPAD(CAST(WEEKOFYEAR(fec_cre) AS STRING), 2, '0')) AS INT)
                        ELSE
                            CAST(CONCAT(CAST(YEAR(fec_cre) AS STRING),LPAD(CAST(WEEKOFYEAR(fec_cre) AS STRING), 2, '0')) AS INT)
                     END,pvt.num_periodo_sem) AS fec_ant_credimax
          FROM PIVOTE AS pvt
          LEFT JOIN
          (SELECT id_master, num_periodo_sem, sld_tot_pendiente_credimax, num_sem_atraso_credimax
                 ,CASE WHEN sld_tot_pendiente_credimax > 0 THEN 1 ELSE 0 END AS ind_sld
           FROM ${esquema_cu}.cu_con_cte_actividad_credimax_aux_sem
           WHERE num_periodo_sem BETWEEN ${num_periodo_sem60} AND ${num_periodo_sem}) AS credimax
          ON pvt.id_master = credimax.id_master AND
             pvt.num_periodo_sem = credimax.num_periodo_sem
          -- Obtenemos la antigüedad
          LEFT JOIN cd_baz_bdclientes.cd_con_cte_antiguedad AS ant
          ON pvt.id_master = ant.id_master) x
    INNER JOIN(SELECT NUM_PERIODO_SEM
                 ,ROW_NUMBER() OVER (ORDER BY NUM_PERIODO_SEM) ID
          FROM cd_baz_bdclientes.cd_gen_fechas_cat
          GROUP BY NUM_PERIODO_SEM) fec_cat
    ON x.max_periodo_sem = fec_cat.num_periodo_sem
),
ESTATUS AS (
     -- Obtenemos los estatus de activo, inactivo y perdido
     SELECT id_master, num_periodo_sem
           ,ind_sld, num_sem_atraso, num_sem_sld_0, fec_ant_credimax
           ,CASE WHEN ind_sld = 1 AND num_sem_atraso <= 39 THEN 1
            ELSE 0 END AS ind_activo_credimax
           ,CASE WHEN ind_sld = 0 AND num_sem_sld_0 <= 8 THEN 1
            ELSE 0 END AS ind_inactivo_credimax
           ,CASE WHEN num_sem_atraso > 39 AND 
           LAG(num_sem_atraso, 1) OVER(PARTITION BY id_master ORDER BY num_periodo_sem) <= 39 
           THEN 1
                 WHEN ind_sld = 0 AND num_sem_sld_0 = 9 
               THEN 1
            ELSE 0 END AS ind_perdido_credimax
     FROM PREVIA_ESTATUS
),
ACTIVIDAD AS (
-- Calculamos las marcas de estatus semanales y a 52 semanas
      SELECT id_master
            ,num_periodo_sem
            ,COALESCE(ind_activo_credimax, 0) AS ind_activo_credimax
            ,COALESCE(IF(ind_perdido_credimax = 1, 0, ind_inactivo_credimax), 0) AS ind_inactivo_credimax -- Para evitar casos raros de clientes sin saldo pendiente pero con semanas de atraso
            ,COALESCE(ind_perdido_credimax, 0) AS ind_perdido_credimax
            ,num_sem_atraso
            ,cast(num_sem_sld_0 as int) as num_sem_sld_0
            ,CASE WHEN est.ind_activo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_credimax >= est.num_periodo_sem THEN '2. Nuevo'
                  WHEN est.ind_activo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_credimax < est.num_periodo_sem THEN '3. Reactivado'
                  WHEN est.ind_inactivo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  WHEN est.ind_perdido_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '5. Perdido'
                  ELSE 'Sin Categoría Semanal' END AS cod_estatus_semanal_credimax
            ,CASE WHEN est.ind_activo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_credimax >= ${num_periodo_sem53} THEN '2. Nuevo'
                  WHEN est.ind_activo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_credimax < ${num_periodo_sem53} THEN '3. Reactivado'
                  WHEN est.ind_inactivo_credimax = 1 AND
                       LAG(est.ind_activo_credimax, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  WHEN (est.ind_perdido_credimax = 1 OR (COALESCE(est.ind_activo_credimax, 0) + COALESCE(est.ind_perdido_credimax, 0) + COALESCE(est.ind_inactivo_credimax, 0) = 0)) AND
                       LAG(est.ind_activo_credimax, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '5. Perdido'
                  ELSE 'Sin Categoría 52sem' END AS cod_estatus_52s_credimax
      FROM ESTATUS AS est
)
SELECT act.id_master
      ,act.ind_activo_credimax
      ,act.ind_inactivo_credimax
      ,act.ind_perdido_credimax
      ,act.num_sem_atraso
      ,cast(act.num_sem_sld_0 as int) as num_sem_sld_0 
      ,act.cod_estatus_semanal_credimax
      ,act.cod_estatus_52s_credimax
      ,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}
;

-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_credimax_sem (
--       id_master                       BIGINT
--      ,ind_activo_credimax             INT
--      ,ind_inactivo_credimax           INT
--      ,ind_perdido_credimax            INT
--      ,num_sem_atraso                  BIGINT
--      ,num_sem_sld_0                   INT
--      ,cod_estatus_semanal_credimax    STRING
--      ,cod_estatus_52s_credimax      STRING
-- ) PARTITIONED BY (num_periodo_sem INT)
-- STORED AS PARQUET;