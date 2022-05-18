"create"
"DROP TABLE IF EXISTS ws_ec_cd_baz_bdclientes.cd_con_cte_actividad_sem; 
CREATE  TABLE ws_ec_cd_baz_bdclientes.cd_con_cte_actividad_sem (
  id_master BIGINT,
  ind_activo_cap INT,
  ind_activo_capnom INT,
  ind_activo_credimax INT,
  ind_activo_nom INT,
  ind_activo_tor INT,
  ind_activo_cre INT,
  ind_activo_rem INT,
  ind_activo_dex INT,
  ind_activo_div INT,
  ind_activo_pre INT,
  ind_activo_pgs INT,
  ind_activo_afr INT,
  ind_perdido_cap INT,
  ind_perdido_capnom INT,
  ind_perdido_credimax INT,
  ind_perdido_nom INT,
  ind_perdido_tor INT,
  ind_perdido_cre INT,
  ind_perdido_rem INT,
  ind_perdido_dex INT,
  ind_perdido_div INT,
  ind_perdido_pre INT,
  ind_perdido_pgs INT,
  ind_perdido_afr INT,
  num_sem_sld_0_credimax INT,
  num_sem_sld_0_nom INT,
  num_sem_sld_0_tor INT,
  num_sem_sld_0_cre INT,
  num_sem_inact_rem INT,
  num_sem_inact_dex INT,
  num_sem_inact_div INT,
  num_sem_inact_pre INT,
  num_sem_inact_pgs INT,
  num_sem_inact_afr INT,
  ind_inactivo_credimax INT,
  ind_inactivo_nom INT,
  ind_inactivo_tor INT,
  ind_inactivo_cre INT,
  ind_inactivo_rem INT,
  ind_inactivo_dex INT,
  ind_inactivo_div INT,
  ind_inactivo_pre INT,
  ind_inactivo_pgs INT,
  ind_inactivo_afr INT,
  cod_estatus_semanal_cap STRING,
  cod_estatus_semanal_capnom STRING,
  cod_estatus_semanal_credimax STRING,
  cod_estatus_semanal_nom STRING,
  cod_estatus_semanal_tor STRING,
  cod_estatus_semanal_cre STRING,
  cod_estatus_semanal_rem STRING,
  cod_estatus_semanal_dex STRING,
  cod_estatus_semanal_div STRING,
  cod_estatus_semanal_pre STRING,
  cod_estatus_semanal_pgs STRING,
  cod_estatus_semanal_afr STRING,
  cod_estatus_semanal_gen STRING,
  cod_estatus_semanal_gen_sin_afr STRING,
  cod_estatus_52s_cap STRING,
  cod_estatus_52s_capnom STRING,
  cod_estatus_52s_credimax STRING,
  cod_estatus_52s_nom STRING,
  cod_estatus_52s_tor STRING,
  cod_estatus_52s_cre STRING,
  cod_estatus_52s_rem STRING,
  cod_estatus_52s_dex STRING,
  cod_estatus_52s_div STRING,
  cod_estatus_52s_pre STRING,
  cod_estatus_52s_pgs STRING,
  cod_estatus_52s_afr STRING,
  cod_estatus_52s_gen STRING,
  cod_estatus_52s_gen_sin_afr STRING,
  ind_activo INT,
  ind_activo_ctes_usua INT,
  ind_activo_solo_ctes INT,
  ind_activo_solo_usua INT,
  ind_activo_sin_afr INT,
  ind_activo_ctes_usua_sin_afr INT,
  ind_activo_solo_ctes_sin_afr INT,
  ind_activo_solo_usua_sin_afr INT,
  ind_inactivo INT,
  ind_inactivo_sin_afr INT,
  ind_perdido INT,
  ind_perdido_sin_afr INT,
  cod_cte_usua STRING,
  cod_cte_usua_sin_afr STRING,
  id_md5 VARCHAR(32) COMMENT 'MD5 DE LA LLAVE DE LA TABLA',
  id_md5completo VARCHAR(32) COMMENT 'MD5COMPLETO DEL REGISTRO COMPLETO',
  fec_carga TIMESTAMP COMMENT 'FECHA AUDITORIA EN QUE SE EJECUTO LA CARGA PARCIAL O COMPLETA LA INFO'
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='169574', 'impala.lastComputeStatsTime'='1650338792', 'numRows'='2456123367', 'totalSize'='31322370653');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_cd_cap_cuenta_hist_sem_part_act; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_cd_cap_cuenta_hist_sem_part_act (
  id_master BIGINT,
  id_cliente VARCHAR(24),
  id_pais CHAR(4),
  id_sucursal VARCHAR(4),
  id_cuenta VARCHAR(14),
  cod_producto VARCHAR(2),
  cod_subprod CHAR(4),
  cod_tipo_moneda CHAR(4),
  sld_diario DECIMAL(22,4),
  fec_fin STRING,
  num_dep_cliente INT,
  num_dep_negocio INT,
  num_ret_cliente INT,
  num_ret_negocio INT
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='138688', 'impala.lastComputeStatsTime'='1649884276', 'numRows'='7644100948', 'totalSize'='212092693883');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_act_cap_trad_sld_dom; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_act_cap_trad_sld_dom (
  id_master BIGINT,
  id_cliente VARCHAR(24),
  id_pais CHAR(4),
  id_sucursal VARCHAR(4),
  id_cuenta VARCHAR(14),
  cod_producto VARCHAR(2),
  cod_subprod CHAR(4),
  cod_tipo_moneda CHAR(4),
  sld_diario DECIMAL(22,4),
  fec_fin STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='138722', 'impala.lastComputeStatsTime'='1649884370', 'numRows'='5304596215', 'totalSize'='141404620104');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_act_cap_trad_txn; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_act_cap_trad_txn (
  id_master BIGINT,
  id_cliente VARCHAR(24),
  id_pais CHAR(4),
  id_sucursal VARCHAR(4),
  id_cuenta VARCHAR(14),
  cod_producto VARCHAR(2),
  cod_subprod CHAR(4),
  cod_tipo_moneda CHAR(4),
  num_dep BIGINT,
  num_ret BIGINT
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='138738', 'impala.lastComputeStatsTime'='1649884469', 'numRows'='5406874915', 'totalSize'='137749511436');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_act_cd_cap_cta_prev; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_act_cd_cap_cta_prev (
  id_master BIGINT,
  id_cliente VARCHAR(24),
  id_pais CHAR(4),
  id_sucursal VARCHAR(4),
  id_cuenta VARCHAR(14),
  cod_producto VARCHAR(2),
  cod_subprod CHAR(4),
  cod_tipo_moneda CHAR(4),
  sld_diario DECIMAL(22,4),
  fec_fin STRING,
  num_dep BIGINT,
  num_ret BIGINT
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='138756', 'impala.lastComputeStatsTime'='1649884614', 'numRows'='5406874915', 'totalSize'='152927926653');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_cd_cap_cuenta_mes_sem_txn_sld_dom; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_cd_cap_cuenta_mes_sem_txn_sld_dom (
  id_master BIGINT,
  id_cliente VARCHAR(24),
  id_pais CHAR(4),
  id_sucursal VARCHAR(4),
  id_cuenta VARCHAR(14),
  cod_producto VARCHAR(2),
  cod_subprod CHAR(4),
  cod_tipo_moneda CHAR(4),
  fec_apertura TIMESTAMP,
  fec_cancelacion TIMESTAMP,
  sld_diario DECIMAL(22,4),
  fec_fin STRING,
  num_dep BIGINT,
  num_ret BIGINT
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='138773', 'impala.lastComputeStatsTime'='1649884746', 'numRows'='5349018139', 'totalSize'='162388847903');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_cap_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_cap_sem (
  id_master BIGINT,
  num_sem_inact INT,
  ind_activo_cap INT,
  ind_inactivo_cap INT,
  ind_perdido_cap INT,
  cod_estatus_semanal_cap STRING,
  cod_estatus_52s_cap STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='138729', 'impala.lastComputeStatsTime'='1649884436', 'numRows'='2383668511', 'totalSize'='14030367210');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_capnom_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_capnom_sem (
  id_master BIGINT,
  num_sem_inact INT,
  ind_activo_capnom INT,
  ind_inactivo_capnom INT,
  ind_perdido_capnom INT,
  cod_estatus_semanal_capnom STRING,
  cod_estatus_52s_capnom STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='148554', 'impala.lastComputeStatsTime'='1650024872', 'numRows'='232994300', 'totalSize'='1536477411');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_cre_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_cre_sem (
  id_master BIGINT,
  ind_activo_credimax INT,
  ind_activo_nom INT,
  ind_activo_tor INT,
  ind_activo_cre INT,
  ind_inactivo_credimax INT,
  ind_inactivo_nom INT,
  ind_inactivo_tor INT,
  ind_inactivo_cre INT,
  ind_perdido_credimax INT,
  ind_perdido_nom INT,
  ind_perdido_tor INT,
  ind_perdido_cre INT,
  num_sem_atraso_credimax BIGINT,
  num_sem_atraso_nom BIGINT,
  num_sem_atraso_tor BIGINT,
  num_sem_atraso_cre BIGINT,
  num_sem_sld_0_credimax INT,
  num_sem_sld_0_nom INT,
  num_sem_sld_0_tor INT,
  num_sem_sld_0_cre INT,
  cod_estatus_semanal_credimax STRING,
  cod_estatus_semanal_nom STRING,
  cod_estatus_semanal_tor STRING,
  cod_estatus_52s_credimax STRING,
  cod_estatus_52s_nom STRING,
  cod_estatus_52s_tor STRING,
  cod_estatus_semanal_cre STRING,
  cod_estatus_52s_cre STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_credimax_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_credimax_sem (
  id_master BIGINT,
  ind_activo_credimax INT,
  ind_inactivo_credimax INT,
  ind_perdido_credimax INT,
  num_sem_atraso BIGINT,
  num_sem_sld_0 INT,
  cod_estatus_semanal_credimax STRING,
  cod_estatus_52s_credimax STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_credimax_aux_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_credimax_aux_sem (
  id_master BIGINT,
  sld_tot_pendiente_credimax DOUBLE,
  num_sem_atraso_credimax BIGINT
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_nom_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_nom_sem (
  id_master BIGINT,
  ind_activo_nom INT,
  ind_inactivo_nom INT,
  ind_perdido_nom INT,
  num_sem_atraso BIGINT,
  num_sem_sld_0 INT,
  cod_estatus_semanal_nom STRING,
  cod_estatus_52s_nom STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_nom_aux_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_nom_aux_sem (
  id_master BIGINT,
  sld_tot_pendiente_nom DOUBLE,
  num_sem_atraso_nom BIGINT
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_tor_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_tor_sem (
  id_master BIGINT,
  ind_activo_tor INT,
  ind_inactivo_tor INT,
  ind_perdido_tor INT,
  num_sem_atraso BIGINT,
  num_sem_sld_0 INT,
  cod_estatus_semanal_tor STRING,
  cod_estatus_52s_tor STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_rem_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_rem_sem (
  id_master BIGINT,
  ind_activo_rem INT,
  ind_perdido_rem INT,
  num_sem_inact INT,
  cod_estatus_semanal_rem STRING,
  cod_estatus_52s_rem STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_dex_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_dex_sem (
  id_master BIGINT,
  ind_activo_dex INT,
  ind_perdido_dex INT,
  num_sem_inact INT,
  cod_estatus_semanal_dex STRING,
  cod_estatus_52s_dex STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_div_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_div_sem (
  id_master BIGINT,
  ind_activo_div INT,
  ind_perdido_div INT,
  num_sem_inact INT,
  cod_estatus_semanal_div STRING,
  cod_estatus_52s_div STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_pre_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_pre_sem (
  id_master BIGINT,
  ind_activo_pre INT,
  ind_perdido_pre INT,
  num_sem_inact INT,
  cod_estatus_semanal_pre STRING,
  cod_estatus_52s_pre STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_pgs_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_pgs_sem (
  id_master BIGINT,
  ind_activo_pgs INT,
  ind_perdido_pgs INT,
  num_sem_inact INT,
  cod_estatus_semanal_pgs STRING,
  cod_estatus_52s_pgs STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_afr_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_afr_sem (
  id_master BIGINT,
  ind_activo_afr INT,
  ind_perdido_afr INT,
  num_sem_inact INT,
  cod_estatus_semanal_afr STRING,
  cod_estatus_52s_afr STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'external.table.purge'='TRUE');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_cd_con_cte_actividad_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_cd_con_cte_actividad_sem (
  id_master BIGINT,
  ind_activo_cap INT,
  ind_activo_capnom INT,
  ind_activo_credimax INT,
  ind_activo_nom INT,
  ind_activo_tor INT,
  ind_activo_cre INT,
  ind_activo_rem INT,
  ind_activo_dex INT,
  ind_activo_div INT,
  ind_activo_pre INT,
  ind_activo_pgs INT,
  ind_activo_afr INT,
  ind_perdido_cap INT,
  ind_perdido_capnom INT,
  ind_perdido_credimax INT,
  ind_perdido_nom INT,
  ind_perdido_tor INT,
  ind_perdido_cre INT,
  ind_perdido_rem INT,
  ind_perdido_dex INT,
  ind_perdido_div INT,
  ind_perdido_pre INT,
  ind_perdido_pgs INT,
  ind_perdido_afr INT,
  num_sem_sld_0_credimax INT,
  num_sem_sld_0_nom INT,
  num_sem_sld_0_tor INT,
  num_sem_sld_0_cre INT,
  num_sem_inact_rem INT,
  num_sem_inact_dex INT,
  num_sem_inact_div INT,
  num_sem_inact_pre INT,
  num_sem_inact_pgs INT,
  num_sem_inact_afr INT,
  ind_inactivo_credimax INT,
  ind_inactivo_nom INT,
  ind_inactivo_tor INT,
  ind_inactivo_cre INT,
  ind_inactivo_rem INT,
  ind_inactivo_dex INT,
  ind_inactivo_div INT,
  ind_inactivo_pre INT,
  ind_inactivo_pgs INT,
  ind_inactivo_afr INT,
  cod_estatus_semanal_cap STRING,
  cod_estatus_semanal_capnom STRING,
  cod_estatus_semanal_credimax STRING,
  cod_estatus_semanal_nom STRING,
  cod_estatus_semanal_tor STRING,
  cod_estatus_semanal_cre STRING,
  cod_estatus_semanal_rem STRING,
  cod_estatus_semanal_dex STRING,
  cod_estatus_semanal_div STRING,
  cod_estatus_semanal_pre STRING,
  cod_estatus_semanal_pgs STRING,
  cod_estatus_semanal_afr STRING,
  cod_estatus_semanal_gen STRING,
  cod_estatus_semanal_gen_sin_afr STRING,
  cod_estatus_52s_cap STRING,
  cod_estatus_52s_capnom STRING,
  cod_estatus_52s_credimax STRING,
  cod_estatus_52s_nom STRING,
  cod_estatus_52s_tor STRING,
  cod_estatus_52s_cre STRING,
  cod_estatus_52s_rem STRING,
  cod_estatus_52s_dex STRING,
  cod_estatus_52s_div STRING,
  cod_estatus_52s_pre STRING,
  cod_estatus_52s_pgs STRING,
  cod_estatus_52s_afr STRING,
  cod_estatus_52s_gen STRING,
  cod_estatus_52s_gen_sin_afr STRING,
  ind_activo INT,
  ind_activo_ctes_usua INT,
  ind_activo_solo_ctes INT,
  ind_activo_solo_usua INT,
  ind_activo_sin_afr INT,
  ind_activo_ctes_usua_sin_afr INT,
  ind_activo_solo_ctes_sin_afr INT,
  ind_activo_solo_usua_sin_afr INT,
  ind_inactivo INT,
  ind_inactivo_sin_afr INT,
  ind_perdido INT,
  ind_perdido_sin_afr INT,
  cod_cte_usua STRING,
  cod_cte_usua_sin_afr STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='169526', 'impala.lastComputeStatsTime'='1650338157', 'numRows'='2456123367', 'totalSize'='30979263755');"
"DROP TABLE IF EXISTS ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_prev_sem; 
CREATE  TABLE ws_ec_cu_baz_bdclientes.cu_con_cte_actividad_prev_sem (
  id_master BIGINT,
  ind_activo_cap INT,
  ind_activo_capnom INT,
  ind_activo_credimax INT,
  ind_activo_nom INT,
  ind_activo_tor INT,
  ind_activo_cre INT,
  ind_activo_rem INT,
  ind_activo_dex INT,
  ind_activo_div INT,
  ind_activo_pre INT,
  ind_activo_pgs INT,
  ind_activo_afr INT,
  ind_perdido_cap INT,
  ind_perdido_capnom INT,
  ind_perdido_credimax INT,
  ind_perdido_nom INT,
  ind_perdido_tor INT,
  ind_perdido_cre INT,
  ind_perdido_rem INT,
  ind_perdido_dex INT,
  ind_perdido_div INT,
  ind_perdido_pre INT,
  ind_perdido_pgs INT,
  ind_perdido_afr INT,
  num_sem_sld_0_credimax INT,
  num_sem_sld_0_nom INT,
  num_sem_sld_0_tor INT,
  num_sem_sld_0_cre INT,
  num_sem_inact_rem INT,
  num_sem_inact_dex INT,
  num_sem_inact_div INT,
  num_sem_inact_pre INT,
  num_sem_inact_pgs INT,
  num_sem_inact_afr INT,
  ind_inactivo_credimax INT,
  ind_inactivo_nom INT,
  ind_inactivo_tor INT,
  ind_inactivo_cre INT,
  ind_inactivo_rem INT,
  ind_inactivo_dex INT,
  ind_inactivo_div INT,
  ind_inactivo_pre INT,
  ind_inactivo_pgs INT,
  ind_inactivo_afr INT,
  cod_estatus_semanal_cap STRING,
  cod_estatus_semanal_capnom STRING,
  cod_estatus_semanal_credimax STRING,
  cod_estatus_semanal_nom STRING,
  cod_estatus_semanal_tor STRING,
  cod_estatus_semanal_cre STRING,
  cod_estatus_semanal_rem STRING,
  cod_estatus_semanal_dex STRING,
  cod_estatus_semanal_div STRING,
  cod_estatus_semanal_pre STRING,
  cod_estatus_semanal_pgs STRING,
  cod_estatus_semanal_afr STRING,
  cod_estatus_52s_cap STRING,
  cod_estatus_52s_capnom STRING,
  cod_estatus_52s_credimax STRING,
  cod_estatus_52s_nom STRING,
  cod_estatus_52s_tor STRING,
  cod_estatus_52s_cre STRING,
  cod_estatus_52s_rem STRING,
  cod_estatus_52s_dex STRING,
  cod_estatus_52s_div STRING,
  cod_estatus_52s_pre STRING,
  cod_estatus_52s_pgs STRING,
  cod_estatus_52s_afr STRING,
  ind_activo INT,
  ind_activo_ctes_usua INT,
  ind_activo_solo_ctes INT,
  ind_activo_solo_usua INT,
  ind_activo_sin_afr INT,
  ind_activo_ctes_usua_sin_afr INT,
  ind_activo_solo_ctes_sin_afr INT,
  ind_activo_solo_usua_sin_afr INT,
  ind_inactivo INT,
  ind_inactivo_sin_afr INT,
  ind_perdido INT,
  ind_perdido_sin_afr INT,
  cod_cte_usua STRING,
  cod_cte_usua_sin_afr STRING
)
PARTITIONED BY (
  num_periodo_sem INT
)
STORED AS PARQUET

TBLPROPERTIES ('DO_NOT_UPDATE_STATS'='true', 'OBJCAPABILITIES'='EXTREAD,EXTWRITE', 'STATS_GENERATED'='TASK', 'external.table.purge'='TRUE', 'impala.events.catalogServiceId'='6d25f3c0d3a46e0:b9f5d4639557bc59', 'impala.events.catalogVersion'='207703', 'impala.lastComputeStatsTime'='1650987917', 'numRows'='6232171614', 'totalSize'='79347870397');"
