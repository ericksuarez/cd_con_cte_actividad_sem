-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Divisas
-- Creamos la tabla con el estatus de actividad/perdidos de Divisas
-- Necesitamos tener la vista de 52 semanas antes del cliente para obtener la bandera de vista anual

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_div_sem
PARTITION(num_periodo_sem)

WITH 
-- Obtenemos los clientes que han estado activos en Divisas desde hace 52 semanas (los que no han estado activos no apareceran en esa semana)
PIVOTE AS (
    SELECT dvs.id_master
           ,semanas.num_periodo_sem
           ,semanas.id
    FROM(SELECT DISTINCT id_master
         FROM ${esquema_cd}.cd_div_cte_hist_sem
         WHERE id_master IS NOT NULL
               AND ind_activo_div = 1
               AND num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS dvs
    CROSS JOIN (SELECT num_periodo_sem, id
                FROM (SELECT NUM_PERIODO_SEM
                       ,ROW_NUMBER() OVER (ORDER BY NUM_PERIODO_SEM) ID
                    FROM cd_baz_bdclientes.cd_gen_fechas_cat
                    GROUP BY NUM_PERIODO_SEM) fec_cat
                WHERE num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS semanas
)
,ESTATUS AS (
    SELECT x.id_master
           ,x.num_periodo_sem
           ,x.ind_activo_div
           ,x.id-fec_cat.id as num_sem_inact
           ,x.fec_ant_div
    FROM (SELECT pvt.id_master
                ,pvt.num_periodo_sem
                ,pvt.id
                ,max(pvt.num_periodo_sem*COALESCE(dvs.ind_activo_div, 0)) OVER(PARTITION BY pvt.id_master) as max_periodo_sem
                ,COALESCE(dvs.ind_activo_div, 0) AS ind_activo_div
                ,COALESCE(
                  CASE
                    WHEN MONTH(ant.fec_dvs) = 1 AND WEEKOFYEAR(ant.fec_dvs) >= 50 THEN CAST(CONCAT(CAST(YEAR(ant.fec_dvs) - 1 AS STRING),LPAD(CAST(WEEKOFYEAR(ant.fec_dvs) AS STRING), 2, '0')) AS INT)
                    WHEN MONTH(ant.fec_dvs) = 12 AND WEEKOFYEAR(ant.fec_dvs) = 1 THEN CAST(CONCAT(CAST(YEAR(ant.fec_dvs) + 1 AS STRING),LPAD(CAST(WEEKOFYEAR(ant.fec_dvs) AS STRING), 2, '0')) AS INT)
                    ELSE
                        CAST(CONCAT(CAST(YEAR(ant.fec_dvs) AS STRING),LPAD(CAST(WEEKOFYEAR(ant.fec_dvs) AS STRING), 2, '0')) AS INT) END
                    , pvt.num_periodo_sem) AS fec_ant_div
          FROM PIVOTE AS pvt
          LEFT JOIN ${esquema_cd}.cd_div_cte_hist_sem AS dvs
          ON pvt.id_master = dvs.id_master AND
             pvt.num_periodo_sem = dvs.num_periodo_sem
          LEFT JOIN cd_baz_bdclientes.cd_con_cte_antiguedad AS ant
          ON pvt.id_master = ant.id_master) x
    INNER JOIN (SELECT NUM_PERIODO_SEM
                       ,ROW_NUMBER() OVER (ORDER BY NUM_PERIODO_SEM) ID
               FROM cd_baz_bdclientes.cd_gen_fechas_cat
               GROUP BY NUM_PERIODO_SEM) fec_cat
    ON x.max_periodo_sem = fec_cat.num_periodo_sem
)
,ACTIVIDAD AS (
      SELECT id_master
            ,num_periodo_sem
            ,ind_activo_div
            ,num_sem_inact
            ,IF(num_sem_inact = 40, 1, 0) AS ind_perdido_div
            ,CASE WHEN est.ind_activo_div = 1 AND
                       LAG(est.ind_activo_div, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_div = 1 AND
                       LAG(est.ind_activo_div, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_div >= est.num_periodo_sem THEN '2. Nuevo'
                  WHEN est.ind_activo_div = 1 AND
                       LAG(est.ind_activo_div, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_div < est.num_periodo_sem THEN '3. Reactivado'
                  WHEN est.ind_activo_div = 0 AND
                       LAG(est.ind_activo_div, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  ELSE 'Sin Categoría Semanal' END AS cod_estatus_semanal_div
            ,CASE WHEN est.ind_activo_div = 1 AND
                       LAG(est.ind_activo_div, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_div = 1 AND
                       LAG(est.ind_activo_div, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_div >= ${num_periodo_sem53} THEN '2. Nuevo'
                  WHEN est.ind_activo_div = 1 AND
                       LAG(est.ind_activo_div, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_div < ${num_periodo_sem53} THEN '3. Reactivado'
                  WHEN est.ind_activo_div = 0 AND
                       LAG(est.ind_activo_div, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact <= 39 THEN '4. Inactivado'
                  WHEN est.ind_activo_div = 0 AND
                       LAG(est.ind_activo_div, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact >= 40 THEN '5. Perdido'
                  ELSE 'Sin Categoría 52sem' END AS cod_estatus_52s_div
      FROM ESTATUS AS est
)
SELECT act.id_master
      ,act.ind_activo_div
      ,act.ind_perdido_div
      ,cast(act.num_sem_inact as int) as num_sem_inact
      ,act.cod_estatus_semanal_div
      ,act.cod_estatus_52s_div
      ,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}
;

-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_div_sem (
--       id_master                  BIGINT
--      ,ind_activo_div             INT
--      ,ind_perdido_div            INT
--      ,num_sem_inact              INT
--      ,cod_estatus_semanal_div    STRING
--      ,cod_estatus_52s_div      STRING
-- ) PARTITIONED BY (num_periodo_sem INT)
-- STORED AS PARQUET;