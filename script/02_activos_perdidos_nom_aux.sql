-- Proceso de Actividad v2.0
-- Oscar Barranco Velásquez <oscar.barranco@gruposalinas.com.mx>

-- Nómina Auxiliar
-- Creamos la tabla con el estatus de actividad/perdidos de Nómina Auxiliar
-- Actualizamos la tabla con el detalle de saldo y semanas de atraso por mes

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_cte_actividad_nom_aux_sem 
PARTITION(num_periodo_sem)

WITH 
DETALLE AS (
     SELECT detalle.id_master
           ,num_periodo_sem
           ,SUM(CASE WHEN BTRIM(cod_nivel1) = 'Nomina' THEN COALESCE(sld_total_pendiente, 0)
                ELSE 0 END) AS sld_tot_pendiente_nom -- Saldo pendiente de Nómina
           ,MAX(CASE WHEN BTRIM(cod_nivel1) = 'Nomina' THEN COALESCE(CAST(num_sem_atraso AS INT), 0)
                ELSE 0 END) AS num_sem_atraso_nom -- Semanas de atraso de credimax
     FROM (SELECT id_pedido_pais
                    ,id_pedido_canal
                    ,id_pedido_sucursal
                    ,id_pedido_num
                    ,est_pedido,sld_total_pendiente,num_sem_atraso
                    ,num_periodo_sem
            FROM cd_baz_bdclientes.cd_cre_historica
            WHERE num_periodo_sem >= (SELECT COALESCE(MAX(NUM_PERIODO_SEM),201852)
                                    FROM ${esquema_cu}.cu_con_cte_actividad_nom_aux_sem)) AS hist -- Tabla con el detalle de los saldos y pedidos a nivel semana
     LEFT JOIN cd_baz_bdclientes.cd_cre_detalle_pedido AS detalle -- Obtenemos el fitir de los pedidos
     ON hist.id_pedido_pais = detalle.id_pedido_pais AND
        hist.id_pedido_canal = detalle.id_pedido_canal AND
        hist.id_pedido_sucursal = detalle.id_pedido_sucursal AND
        hist.id_pedido_num = detalle.id_pedido_num
     LEFT JOIN ws_ec_tmp_baz_bdclientes.cd_cre_fitires_cat_sie AS cat -- Obtenemos el nivel de los pedidos (para filtrar CREDIMAX)
     ON detalle.cod_fitir = CAST(cat.fitir_homologado AS INT) -- Fitir corregido por que vienen algunos como string en el catálogo del SIE
     WHERE hist.est_pedido = 1 -- Solo pedidos surtidos
       -- AND hist.num_periodo_sem IN (SELECT num_periodo_sem FROM MES_SEM) -- Para filtrar las semanas que vamos a ocupar (Se filtran en el inner join)
       AND BTRIM(cat.cod_nivel1) = 'Nomina' -- Nomina
     GROUP BY detalle.id_master, num_periodo_sem
)

SELECT id_master
      ,sld_tot_pendiente_nom
      ,num_sem_atraso_nom
      ,num_periodo_sem
FROM DETALLE;

-- DDL
-- CREATE TABLE ${esquema_cu}.cu_con_cte_actividad_nom_aux_sem (
--      id_master                            BIGINT
--     ,sld_tot_pendiente_nom                DOUBLE
--     ,num_sem_atraso_nom                   BIGINT
-- ) PARTITIONED BY (num_periodo_sem INT)
-- STORED AS PARQUET;