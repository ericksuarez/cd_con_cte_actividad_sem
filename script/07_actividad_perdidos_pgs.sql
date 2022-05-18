-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Programas Sociales
-- Creamos la tabla con el estatus de actividad/perdidos de pgsesas
-- Necesitamos tener la vista de 52 semanas antes del cliente para obtener la bandera de vista 52s

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_pgs_sem
PARTITION(num_periodo_sem)
WITH 
-- Obtenemos los clientes que han estado activos en PGS desde hace 52 semanas (los que no han estado activos no apareceran en esa semana)
PIVOTE AS (
    SELECT pgs.id_master
           ,semanas.num_periodo_sem
           ,semanas.id
    FROM
    (SELECT DISTINCT id_master
     FROM ${esquema_cd}.cd_pgs_cte_hist_sem
     WHERE id_master IS NOT NULL
       AND num_disp_26s > 0
       AND num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS pgs

    CROSS JOIN
    (SELECT num_periodo_sem, id
    FROM (SELECT NUM_PERIODO_SEM
               ,ROW_NUMBER() OVER (ORDER BY NUM_PERIODO_SEM) ID
          FROM cd_baz_bdclientes.cd_gen_fechas_cat
          GROUP BY NUM_PERIODO_SEM) fec_cat
     WHERE num_periodo_sem BETWEEN ${num_periodo_sem53} AND ${num_periodo_sem}) AS semanas
)
,ESTATUS AS (
    SELECT x.id_master
           ,x.num_periodo_sem
           ,x.ind_activo_pgs
           ,x.id-fec_cat.id as num_sem_inact
           ,x.fec_ant_pgs
    FROM (SELECT pvt.id_master
              ,pvt.num_periodo_sem
              ,COALESCE(IF(pgs.num_disp_26s > 0, 1, 0), 0) AS ind_activo_pgs
              ,pvt.id
              ,max(pvt.num_periodo_sem*(COALESCE(IF(pgs.num_disp_26s > 0, 1, 0), 0))) OVER(PARTITION BY pvt.id_master) AS max_periodo_sem
              ,COALESCE(
                  CASE
                    WHEN MONTH(ant.fec_pgs) = 1 AND WEEKOFYEAR(ant.fec_pgs) >= 50 -- DIC/ENE
                        THEN CAST(CONCAT(CAST(YEAR(ant.fec_pgs) - 1 AS STRING),LPAD(CAST(WEEKOFYEAR(ant.fec_pgs) AS STRING), 2, '0')) AS INT)
                    WHEN MONTH(ant.fec_pgs) = 12 AND WEEKOFYEAR(ant.fec_pgs) = 1 -- ENE/DIC
                        THEN CAST(CONCAT(CAST(YEAR(ant.fec_pgs) + 1 AS STRING),LPAD(CAST(WEEKOFYEAR(ant.fec_pgs) AS STRING), 2, '0')) AS INT)
                    ELSE
                        CAST(CONCAT(CAST(YEAR(ant.fec_pgs) AS STRING),LPAD(CAST(WEEKOFYEAR(ant.fec_pgs) AS STRING), 2, '0')) AS INT)
                 END, pvt.num_periodo_sem) AS fec_ant_pgs
        FROM PIVOTE AS pvt
        LEFT JOIN ${esquema_cd}.cd_pgs_cte_hist_sem AS pgs
        ON pvt.id_master = pgs.id_master AND
           pvt.num_periodo_sem = pgs.num_periodo_sem
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
            ,ind_activo_pgs
            ,num_sem_inact
            ,IF(num_sem_inact = 14, 1, 0) AS ind_perdido_pgs
            ,CASE WHEN est.ind_activo_pgs = 1 AND
                       LAG(est.ind_activo_pgs, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_pgs = 1 AND
                       LAG(est.ind_activo_pgs, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pgs >= est.num_periodo_sem THEN '2. Nuevo'
                  WHEN est.ind_activo_pgs = 1 AND
                       LAG(est.ind_activo_pgs, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pgs < est.num_periodo_sem THEN '3. Reactivado'
                  WHEN est.ind_activo_pgs = 0 AND
                       LAG(est.ind_activo_pgs, 1) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '4. Inactivado'
                  ELSE 'Sin Categoría Semanal' END AS cod_estatus_semanal_pgs
            ,CASE WHEN est.ind_activo_pgs = 1 AND
                       LAG(est.ind_activo_pgs, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 THEN '1. Se Mantiene Activo'
                  WHEN est.ind_activo_pgs = 1 AND
                       LAG(est.ind_activo_pgs, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pgs >= ${num_periodo_sem53} THEN '2. Nuevo'
                  WHEN est.ind_activo_pgs = 1 AND
                       LAG(est.ind_activo_pgs, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 0 AND
                       est.fec_ant_pgs < ${num_periodo_sem53} THEN '3. Reactivado'
                  WHEN est.ind_activo_pgs = 0 AND
                       LAG(est.ind_activo_pgs, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact <= 13 THEN '4. Inactivado'
                  WHEN est.ind_activo_pgs = 0 AND
                       LAG(est.ind_activo_pgs, 52) OVER(PARTITION BY est.id_master ORDER BY est.num_periodo_sem) = 1 AND
                       est.num_sem_inact > 13 THEN '5. Perdido'
                  ELSE 'Sin Categoría 52sem' END AS cod_estatus_52s_pgs
      FROM ESTATUS AS est
)
SELECT act.id_master
      ,act.ind_activo_pgs
      ,act.ind_perdido_pgs
      ,cast(act.num_sem_inact as int) as num_sem_inact 
      ,act.cod_estatus_semanal_pgs
      ,act.cod_estatus_52s_pgs
      ,act.num_periodo_sem
FROM ACTIVIDAD AS act
WHERE num_periodo_sem = ${num_periodo_sem}
;

-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_pgs_sem (
--       id_master                  BIGINT
--      ,ind_activo_pgs             INT
--      ,ind_perdido_pgs            INT
--      ,num_sem_inact              INT
--      ,cod_estatus_semanal_pgs    STRING
--      ,cod_estatus_52s_pgs      STRING
-- ) PARTITIONED BY (num_periodo_sem INT)
-- STORED AS PARQUET;