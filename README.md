## In order to use this program you need to:
1) Create 2 files in src/main/input/ path: table_name.conf and table_name.txt
   <br>(If input does not exists, create it);
2) Copy and paste DDL from the database into table_name.txt;
3) Insert the appropriate configurations in the talbe_name.conf file;
4) Run main class.

If you are uncertain about how to do it, you can take example from the files in the 
exampleFile folder or you can look at #create-txt-and-conf-files section.

## In conf file 
- if historization_columns field is empty it will be replaced with checkColumn field
####be carefull to:
- do not leave whitespaces in values, use double quotes instead.
<br>es: historization_columns="";
- if the value is composed by two or more fields use double quotes.
<br>es: historization_columns="s_cliente,x_rag_soc"

## In .txt file
- txt file must contains "CONSTRAINT" line
## Supported ingestionMode:
- FULL;
- DELTA_DATE.

## Supported types:
- int;
- smallint;
- bigint;
- numeric;
- decimal;
- float;
- datetime;
- datetime2 (as datetime);
- varchar;
- bit.

## Supported accents:
- à; À;
- è; È;
- é; É;
- ì; Ì;
- ò; Ì;
- ù; Ù.

<br>

### SSH Hive file
#### ${hivevar:hive_db} and ${environment.datalake.hdfs.uri} variables
If you want to create the three hive files in order to copy and paste them into hive pront 
command, you can use hiveDbReplacer class.<br>
It will create a copy of hive tables with these two variables replaced. <br>
Be sure you replace the correct variable in hiveDbVarReplacer.conf (src/main/scala/hiveDbVarReplacer.conf). <br>
You can fine the files in "src/main/output/hiveValueReplaced" path.
<br>
<br>


---
# Create txt and conf files
<br><br>

### DELTA_DATE: talbe_name.conf example
databaseName=summer_Dati_telelettura.dbo<br>
tableName=Press_Temp_Energ_Telelette<br>
sourceSystemName=telelettura<br>

ingestionMode=DELTA_DATE<br>
checkColumn=data_ultimo_aggiornamento<br>
POSSIBLE_PHYSICAL_DELETES=false<br>

historizationFlag=false<br>
historization_columns=""<br>
HISTORICIZATION_ORDERING_COLUMN=""<br>

partitioningFlag=true<br>
dateColumn=data_trace_quartoraria<br>
curatedPartitioningColumn=annomese INT<br>
---
### FULL: talbe_name.conf example
databaseName=summer_consistenze_imp.dbo<br>
tableName=pressioni_portate_linee<br>
sourceSystemName=anagrafica<br>

ingestionMode=FULL<br>
checkColumn=d_caricamento<br>
POSSIBLE_PHYSICAL_DELETES=true<br>

historizationFlag=false<br>
historization_columns="tipologia_severita,descrizione_messaggio_diagnostica"<br>
HISTORICIZATION_ORDERING_COLUMN=""<br>

partitioningFlag=false<br>
dateColumn=""<br>
curatedPartitioningColumn=""<br>

---
### table_name.txt example

remi_assoluto varchar(8) COLLATE Latin1_General_CI_AS NOT NULL,<br>
num_linee_misura int NOT NULL,<br>
data_trace_quartoraria datetime NOT NULL,<br>
id_flow_computer varchar(8) COLLATE Latin1_General_CI_AS NOT NULL,<br>
pressione float NULL,<br>
unita_misura_pressione varchar(8) COLLATE Latin1_General_CI_AS NULL,<br>
temperatura numeric(8,5) NULL,<br>
unita_misura_temperatura varchar(8) COLLATE Latin1_General_CI_AS NULL,<br>
energia numeric(8,5) NULL,<br>
unita_misura_energia varchar(8) COLLATE Latin1_General_CI_AS NULL,<br>
data_ultimo_aggiornamento datetime NULL,<br>
utente_ultimo_aggiornamento varchar(10) COLLATE Latin1_General_CI_AS NULL,<br>
tipo_pressione_originale varchar(1) COLLATE Latin1_General_CI_AS NULL,<br>
pressione_barometrica float NULL,<br>
CONSTRAINT PK__Press_Te__800C7E7A2AED02FD PRIMARY KEY (remi_assoluto,num_linee_misura,data_trace_quartoraria,id_flow_computer)

---
### hiveDbVarReplacer.conf

environment_datalake_hdfs_uri = "abfs://data@noprodcdp.dfs.core.windows.net"<br>
raw_hive_db=1707_summer_r_svil<br>
curated_hive_db=1707_summer_c_svil<br>
integrated_hive_db=1742_kpi_svil<br>

---