-- Obtencion de l ultimo perido de la tabla de cta hist, 
INSERT OVERWRITE TABLE ${esquema_cu}.cu_cd_cap_cuenta_hist_sem_part_act 
PARTITION(num_periodo_sem)
SELECT 
	 ID_MASTER
	,ID_CLIENTE
	,ID_PAIS
	,ID_SUCURSAL
	,ID_CUENTA
	,COD_PRODUCTO
	,COD_SUBPROD
	,COD_TIPO_MONEDA
	,SLD_DIARIO
	,TO_DATE(FEC_FIN) AS FEC_FIN
	,NUM_DEP_CLIENTE
	,NUM_DEP_NEGOCIO
	,NUM_RET_CLIENTE
	,NUM_RET_NEGOCIO
	,NUM_PERIODO_SEM
FROM ws_ec_cd_baz_bdclientes.cd_cap_cuenta_hist_sem
WHERE num_periodo_sem = ${num_periodo_sem}
;

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_cd_cap_cuenta_hist_sem_part_act;

-- Obtencion del saldo al corte del Domingo
INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_act_cap_trad_sld_dom
PARTITION(num_periodo_sem)
SELECT  
	 ID_MASTER
	,ID_CLIENTE
	,ID_PAIS
	,ID_SUCURSAL
	,ID_CUENTA
	,COD_PRODUCTO
	,COD_SUBPROD
	,COD_TIPO_MONEDA
	,SLD_DIARIO
    ,FEC_FIN
    ,A.NUM_PERIODO_SEM
FROM ${esquema_cu}.cu_cd_cap_cuenta_hist_sem_part_act A
INNER JOIN(
    SELECT
         NUM_PERIODO_SEM
        ,TO_DATE(MAX(FEC_STRING)) AS CORTE	
    FROM cd_baz_bdclientes.cd_gen_fechas_cat 
    GROUP BY NUM_PERIODO_SEM
) B ON
A.fec_fin = B.CORTE
WHERE a.num_periodo_sem = ${num_periodo_sem}
;

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_con_act_cap_trad_sld_dom;

-- Obtencion del numero de transacciones durante la semana
INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_act_cap_trad_txn
PARTITION(num_periodo_sem)
SELECT  
	 ID_MASTER
	,ID_CLIENTE
	,ID_PAIS
	,ID_SUCURSAL
	,ID_CUENTA
	,COD_PRODUCTO
	,COD_SUBPROD
	,COD_TIPO_MONEDA
	,SUM(NUM_DEP_CLIENTE + NUM_DEP_NEGOCIO) 										AS NUM_DEP
	,SUM(NUM_RET_CLIENTE + NUM_RET_NEGOCIO) 										AS NUM_RET
	,NUM_PERIODO_SEM
FROM ${esquema_cu}.cu_cd_cap_cuenta_hist_sem_part_act
WHERE num_periodo_sem = ${num_periodo_sem}
GROUP BY 
	 ID_MASTER
	,ID_CLIENTE
	,ID_PAIS
	,ID_SUCURSAL
	,ID_CUENTA
	,COD_PRODUCTO
	,COD_SUBPROD
	,COD_TIPO_MONEDA
	,NUM_PERIODO_SEM
;

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_con_act_cap_trad_txn;

INSERT OVERWRITE TABLE ${esquema_cu}.cu_con_act_cd_cap_cta_prev
PARTITION(num_periodo_sem)
SELECT  
	 ID_MASTER
	,ID_CLIENTE
	,ID_PAIS
	,ID_SUCURSAL
	,ID_CUENTA
	,COD_PRODUCTO
	,COD_SUBPROD
	,COD_TIPO_MONEDA
	,MAX(SLD_DIARIO) AS SLD_DIARIO
	,MAX(FEC_FIN)    AS FEC_FIN
	,MAX(NUM_DEP)    AS NUM_DEP
	,MAX(NUM_RET)    AS NUM_RET
	,NUM_PERIODO_SEM
FROM(
	SELECT ID_MASTER ,ID_CLIENTE ,ID_PAIS ,ID_SUCURSAL ,ID_CUENTA ,COD_PRODUCTO ,COD_SUBPROD ,COD_TIPO_MONEDA ,0 AS SLD_DIARIO ,"" AS FEC_FIN ,NUM_DEP ,NUM_RET ,NUM_PERIODO_SEM 
	FROM ${esquema_cu}.cu_con_act_cap_trad_txn 
	WHERE num_periodo_sem = ${num_periodo_sem}
	
	UNION ALL
	
	SELECT ID_MASTER ,ID_CLIENTE ,ID_PAIS ,ID_SUCURSAL ,ID_CUENTA ,COD_PRODUCTO ,COD_SUBPROD ,COD_TIPO_MONEDA ,SLD_DIARIO ,FEC_FIN ,0 AS NUM_DEP ,0 AS NUM_RET ,NUM_PERIODO_SEM 
	FROM ${esquema_cu}.cu_con_act_cap_trad_sld_dom 
	WHERE num_periodo_sem = ${num_periodo_sem}
	) A
GROUP BY 
	 ID_MASTER
	,ID_CLIENTE
	,ID_PAIS
	,ID_SUCURSAL
	,ID_CUENTA
	,COD_PRODUCTO
	,COD_SUBPROD
	,COD_TIPO_MONEDA
	,NUM_PERIODO_SEM
;	

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_con_act_cd_cap_cta_prev;	

INSERT OVERWRITE TABLE ${esquema_cu}.cu_cd_cap_cuenta_mes_sem_txn_sld_dom
PARTITION(num_periodo_sem)
SELECT  
	 A.ID_MASTER
	,A.ID_CLIENTE
	,A.ID_PAIS
	,A.ID_SUCURSAL
	,A.ID_CUENTA
	,A.COD_PRODUCTO
	,A.COD_SUBPROD
	,A.COD_TIPO_MONEDA
	,FEC_APERTURA
	,FEC_CANCELACION
	,SLD_DIARIO
	,FEC_FIN
	,NUM_DEP
	,NUM_RET
	,NUM_PERIODO_SEM
FROM ${esquema_cu}.cu_con_act_cd_cap_cta_prev A
JOIN(
	SELECT
		 ID_PAIS
		,ID_SUCURSAL
		,ID_CUENTA
		,COD_SUCURSAL_CTA
		,COD_TIPO_PERSONA
		,from_utc_timestamp(FEC_APERTURA ,'Mexico/General')     AS FEC_APERTURA
		,from_utc_timestamp(FEC_CANCELACION, 'Mexico/General')  AS FEC_CANCELACION
		,num_periodo_sem1s AS NUM_PERIODO_SEM_CANCELACION --suma 1 semana a la fecha de cancelacion con el catalogo
	FROM ${esquema_cd}.cd_cap_cuenta_sem
	LEFT JOIN ws_ec_cd_baz_bdclientes.cd_gen_fechas_params_cat SEM ON
	fec_cancelacion = SEM.fec_string
	WHERE COD_TITULAR = 'T'
	) C ON 
	A.ID_PAIS = C.ID_PAIS           AND
	A.ID_SUCURSAL = C.ID_SUCURSAL   AND
	A.ID_CUENTA = C.ID_CUENTA
WHERE A.num_periodo_sem = ${num_periodo_sem} AND
	(FEC_CANCELACION IS NULL OR 
	 C.NUM_PERIODO_SEM_CANCELACION > A.NUM_PERIODO_SEM)
;

COMPUTE INCREMENTAL STATS ${esquema_cu}.cu_cd_cap_cuenta_mes_sem_txn_sld_dom;