## In order to use this program you need to:
1) Create 2 files in src/main/InputFolder/ path: table_name.conf and table_name.txt
<br>(If InputFolder does not exists create it);
2) Copy and paste DDL from the database into table_name.txt;
3) Insert the appropriate configurations in the talbe_name.conf file;
4) Run main class.

If you are uncertain about how to do it, you can take example from the files in the 
exampleFile folder.

## In conf file be carefull to:
- do not leave whitespaces in values, use double quotes instead
<br>es: historization_columns="";
- if the value is composed by two or more fields use double quotes
<br>es: historization_columns="s_cliente,x_rag_soc"

## 

<br><br>

### SSH Hive file
#### ${hivevar:hive_db} and ${environment.datalake.hdfs.uri} variables
If you want to create the three hive files in order to copy and paste them in hive pront 
command you can use hiveDbReplacerMain class.<br>
It will create three more files with these two variables replaced. <br>
Be sure you replace the correct variable in hiveDbVarReplacer.conf.<br>
Before using this class you will need to follow the previous steps (run main class)!