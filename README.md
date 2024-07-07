# Pipeline de Datos Streaming con Databricks

## 1. Descripción del Proyecto

Este proyecto muestra la implementación de un pipeline de datos utilizando Databricks, Kafka y Delta Lake en Azure. El objetivo es demostrar cómo se pueden integrar diversas tecnologías para crear un flujo de datos en tiempo real, desde la generación de datos hasta su almacenamiento y procesamiento. El proyecto incluye la creación de una base de datos para almacenar información de alumnos y la simulación de datos ficticios de alumnos que se ingestan cada dos segundos en Kafka para simular un streaming, actualizando las tablas Delta Lake en tiempo real.

## 2. Estructura del Proyecto

El proyecto está organizado en las siguientes secciones:

- **Create_Database:** Configuración inicial de la base de datos y las tablas en Databricks.
- **Producer_Messager**: Generación de datos ficticios y envío a un tópico de Kafka.
- **Streaming_Messages:** Consumo de datos en tiempo real desde Kafka y almacenamiento en tablas Delta.

## 3. Tecnologías Utilizadas

### **Databricks**

Databricks es una plataforma de análisis de datos unificada que simplifica y acelera la realización de tareas de big data. Utilizamos Databricks para:

- Configurar y gestionar el entorno de datos.
- Crear y gestionar bases de datos y tablas Delta lake.
- Ejecutar procesos de ETL y análisis de datos en tiempo real.
- Automatizar la ejecución del pipeline a través de jobs en Databricks.

### **Kafka**

Kafka es una plataforma de streaming de eventos distribuida utilizada para la construcción de pipelines de datos en tiempo real. En este proyecto, Kafka se utiliza para:

- Transmitir datos generados de manera continua.
- Asegurar la entrega eficiente y escalable de mensajes de datos entre sistemas.

### **Delta Lake**

Delta Lake es una capa de almacenamiento que añade fiabilidad a los data lakes. Con Delta Lake, podemos realizar:

- Almacenamiento transaccional ACID.
- Gestión eficiente de grandes volúmenes de datos.
- Integración con Databricks para análisis y procesamiento de datos.

### **Azure Key Vault**

Azure Key Vault es un servicio de gestión de secretos en la nube que permite almacenar y acceder a contraseñas, claves API y otros secretos de manera segura. En este proyecto, Azure Key Vault se utiliza para:

- Gestionar las credenciales de Kafka de manera segura.
- Integrar con Databricks a través de scopes para acceder a los secretos durante la ejecución del pipeline.

## **4. Descripción de los Notebooks**

### **Create_Database**

Este notebook configura la base de datos y las tablas necesarias para almacenar los datos de alumnos. Se elimina cualquier tabla existente para asegurar una configuración limpia y se crean tablas con propiedades específicas como el Change Data Feed (CDF).

	%python
	# Define la base de datos
	database_name = "db_alumnos"

	# Listar todas las tablas en la base de datos
	tables = spark.sql(f"SHOW TABLES IN {database_name}")

	# Eliminar todas las tablas en la base de datos
	for table in tables.collect():
		table_name = table['tableName']
		spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")

	drop database if exists db_alumnos;

	create database db_alumnos;

	use database db_alumnos;

	drop table if exists db_alumnos.tbalumnos;

	%sql
	create table db_alumnos.tbalumnos (
		dni string,
		nombres string,
		curso string,
		nota integer,
		fechaRegistro string,
		fechaRegistroKafka timestamp
	) tblproperties (delta.enableChangeDataFeed = true);

	%sql
	create table db_alumnos.tbalumnos_unique (
		dni string,
		nombres string,
		curso string,
		nota integer,
		fechaRegistro string,
		fechaRegistroKafka timestamp
	) tblproperties (delta.enableChangeDataFeed = true);

### **Producer_Messager**

Este notebook genera datos ficticios utilizando la librería Faker y los envía a un tópico de Kafka cada dos segundos. Los secretos de conexión a Kafka se gestionan mediante Azure Key Vault y se acceden a través de scopes de Databricks. La generación y envío de datos se realiza de manera eficiente y segura.

	%pip install confluent_kafka
	%pip install Faker

	from confluent_kafka import Producer
	from faker import Faker
	import time
	import random

	UsuarioKafka = dbutils.secrets.get(scope='sc-adls', key='UsuarioKafka')
	PasswordKafka = dbutils.secrets.get(scope='sc-adls', key='PasswordKafka')
	ServerKafka = dbutils.secrets.get(scope='sc-adls', key='ServerKafka')

	# Configuración del productor de Kafka
	conf = {
		'bootstrap.servers': ServerKafka,
		'security.protocol': 'SASL_SSL',
		'sasl.mechanisms': 'PLAIN',
		'sasl.username': UsuarioKafka,
		'sasl.password': PasswordKafka,
	}

	producer = Producer(conf) # Envio de mensajes al topico de kafka
	fake = Faker()
	topic = 'notasAlumnos'

	def mensajes_entrega(msg_error, msg):
		if msg_error is not None:
			print('Mensaje de error: {}'.format(msg_error))

	def generar_datos(num_registros):
		cursos = ['apache_spark', 'kafka', 'databricks', 'Data Factory', 'Hadoop']
		datos = []
		for _ in range(num_registros):
			dni = ''.join([str(random.randint(0, 4)) for _ in range(4)])
			fullName = fake.name()
			curso = random.choice(cursos)
			nota = random.randint(7, 20)
			timestamp = fake.date_time_this_decade()
			datos.append({
				'dni': dni,
				'nombres': fullName,
				'curso': curso,
				'nota': nota,
				'fechaRegistro': timestamp.strftime('%m/%d/%Y %H:%M')
			})
			producer.produce(topic, key=fake.uuid4(), value=str(datos[-1]), callback=mensajes_entrega)
			producer.poll(0)
			time.sleep(1)

		producer.flush()
		return datos

	# Generar datos
	datos_generados = generar_datos(10)


### **Streaming_Messages**

Este notebook consume los mensajes de Kafka en tiempo real utilizando la librería readStream de PySpark y los almacena en tablas Delta en Databricks. Además, se configura un job en Databricks para automatizar la ejecución del pipeline y asegurar el procesamiento continuo de los datos. Se implementa lógica de procesamiento para mantener solo los registros más recientes en una tabla de almacenamiento.

	from pyspark.sql.functions import *
	from pyspark.sql.window import Window
	from delta.tables import *

	UsuarioKafka = dbutils.secrets.get(scope='sc-adls', key='UsuarioKafka')
	PasswordKafka = dbutils.secrets.get(scope='sc-adls', key='PasswordKafka')
	ServerKafka = dbutils.secrets.get(scope='sc-adls', key='ServerKafka')

	schema = "struct<dni:string,nombres:string,curso:string, nota:integer, fechaRegistro:string>"

	dfStreamingKafka = (
		spark.readStream.format("kafka")
			.option("subscribe","notasAlumnos")
			.option("kafka.security.protocol", "SASL_SSL") 
			.option("kafka.sasl.jaas.config",
					f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{UsuarioKafka}' password='{PasswordKafka}';") 
			.option("kafka.sasl.mechanism", "PLAIN") 
			.option("kafka.bootstrap.servers", ServerKafka) 
			.option("startingOffsets", "earliest") 
			.load()
			.withColumn("value", col("value").cast("string"))
			.withColumn("value", from_json( "value" ,  schema))
			.select("value.*", "topic", current_timestamp().alias("fechaRegistroKafka"))
	).dropna().drop("topic")

	(
		dfStreamingKafka.writeStream
			.trigger(processingTime='1 seconds')
			.option("checkpointLocation", "dbfs:/user/hive/warehouse/db_alumnos.db/tbalumnos/checkpoint/alumnos")
			.outputMode("append")
			.toTable("db_alumnos.tbalumnos")
	)

	dfStreamingtbalumnos = (
		spark.readStream.table("db_alumnos.tbalumnos")
	)

	def foreachBatchMethod(dfMicrobatch, id):
		windowsSpec = Window.partitionBy("dni" , "curso").orderBy(col("fechaRegistroKafka").desc())
		dfUniques = dfMicrobatch.withColumn("ranked" , row_number().over(windowsSpec)) \
								.where( col("ranked") == 1) \
								.drop("ranked")

		main_table = DeltaTable.forName(spark, "db_alumnos.tbalumnos_unique")

		main_table.alias("main") \
			.merge(dfUniques.alias("news") , "main.curso = news.curso and main.dni = news.dni" )\
			.whenMatchedUpdateAll()\
			.whenNotMatchedInsertAll()\
			.execute()

	(
		dfStreamingtbalumnos.writeStream
			.trigger(processingTime='10 seconds')
			.option("checkpointLocation", "dbfs:/user/hive/warehouse/db_alumnos.db/tbalumnos/checkpoint/alumnos_unique")
			.outputMode("update")
			.foreachBatch(foreachBatchMethod)
			.start()    
	)

## 5. Cómo Ejecutar el Proyecto

### **1. Configurar Databricks:**

- Crear un clúster en Databricks.
- Configurar los secretos de Kafka en Azure Key Vault y crear un scope en Databricks para acceder a ellos.

### **2. Ejecutar los Notebooks:**

- Ejecutar el notebook Create_Database para configurar la base de datos y las tablas.
- Ejecutar el notebook Producer_Messager para generar y enviar datos a Kafka.
- Ejecutar el notebook Streaming_Messages para consumir y procesar los datos en tiempo real.

### **3. Automatizar el Pipeline:**

- Configurar y activar un job en Databricks para automatizar la ejecución de los notebooks en el orden correcto.

### **4. Verificar Resultados:**

- Revisar las tablas tbalumnos y tbalumnos_unique en Databricks para ver los datos procesados.

## Instalación y Configuración:

Sigue las instrucciones a continuación para configurar los servicios necesarios:

#### Apache Kafka:

- Crea una cuenta en Confluent Cloud (o cualquier otro proveedor de Kafka en la nube).

	https://confluent.cloud/environments
	
- Configura un clúster de Kafka y crea los tópicos necesarios.

### Databricks:

- Crea una cuenta en Databricks.

- Configura un clúster y crea un notebook para el procesamiento de datos.

### Delta Lake:

- Utiliza Delta Lake dentro de Databricks para el almacenamiento y procesamiento de datos.

## Configuración de Apache Kafka en Confluent Cloud

### Crear una API Key

- En el dashboard de tu clúster, ve a "API keys".
- Haz clic en "Create Key".

[![API-keys.png](https://i.postimg.cc/NGWzdXD9/API-keys.png)](https://postimg.cc/V5Wg6d3m)

- Selecciona el clúster para el cual deseas crear la API key.
- Haz clic en "Next" y luego coloca una descripción a la API key.

[![Next-api.png](https://i.postimg.cc/1zjHfxYn/Next-api.png)](https://postimg.cc/Z963MQm4)

- Copia y guarda la API Key y el API Secret en un lugar seguro, los necesitarás para configurar Databricks.

[![3-API-keys.png](https://i.postimg.cc/GpxRCQM5/3-API-keys.png)](https://postimg.cc/5HyT5wWB)

### Copiar la cadena del servidor

- En el dashboard de tu clúster, ve a "Cluster settings".

- Copia la cadena del servidor que se encuentra en la sección "Endpoints".

[![4-cadena-conexion.png](https://i.postimg.cc/VvLnRDTQ/4-cadena-conexion.png)](https://postimg.cc/YGZv2fMb)

### Crear un tópico

- En el dashboard de tu clúster, ve a "Topics".
- Haz clic en "Create Topic".
- Ingresa un nombre para tu tópico, para el proyecto lo llamaremos **notasAlumnos**
- Configura las particiones y otras opciones según tus necesidades.
- Haz clic en "Create with defaults" para crear el tópico con la configuración predeterminada.

[![5-create-topic.png](https://i.postimg.cc/zB93LprF/5-create-topic.png)](https://postimg.cc/qhxpSsgN)

[![6-create-topic.png](https://i.postimg.cc/nh0zKnxz/6-create-topic.png)](https://postimg.cc/w1Rgg878)

# CREACIÓN DE GRUPO DE RECURSOS EN AZURE PORTAL

1 . Iniciar Sesión en Azure Portal y crear grupo de recursos

- **Nombre grupo de recursos:** rg-datapath-synapse-002

- **Region**: (US) East US 2

[![crear-grupo-recursos.png](https://i.postimg.cc/cC4xSXX0/crear-grupo-recursos.png)](https://postimg.cc/YGZKNN5s)

2 . Ingresar al grupo de recursos creado, y en la parte superior seleccionar “Create"

[![2-ingresar-RG.png](https://i.postimg.cc/L4T7gH6y/2-ingresar-RG.png)](https://postimg.cc/NLKbWvXT)

# CREACIÓN AZURE KEY VAULT

1.- Ir al grupo de recursos creado **"rg-datapath-synapse-002"**, luego ir a **“+ Create”**, finalmente en el Marketplace escribir **“key vault”** y seleccionar **“Create"**

[![17-create-key-vault.png](https://i.postimg.cc/nzHpmqqh/17-create-key-vault.png)](https://postimg.cc/McrC2MsC)

2.- En la sección **“Basics”** configurar lo siguiente:

- **a. Resource group:** rg-datapath-synapse-002

- **b. Key vault Name:** kv-datapath-jc

- **c. Region: **East US 2

- **d. Pricing Tier:** Standard

[![18-create-key-vault-optons.png](https://i.postimg.cc/xdVbWFVp/18-create-key-vault-optons.png)](https://postimg.cc/VJ46tg6X)

Clic en **“Review + Create” —> “Create” —> “Go to resource”**

# OTORGAR PERMISOS

1.- Ir a “Access control (IAM), luego **“Add”**, clic en **“Add role assignment”**

[![19-add-roles.png](https://i.postimg.cc/Vs04Qpxm/19-add-roles.png)](https://postimg.cc/XX399HNz)

2.- En **“Job function roles”** seleccionar **“key vault contributor”**, luego clic en**“Members”**

[![20-add-roles-member.png](https://i.postimg.cc/W33Wk4Yk/20-add-roles-member.png)](https://postimg.cc/Y45zctHr)

3.- En **“Assign access to”** seleccionar **‘User, group, or service principal’**, luego en **“Members”** clic en **‘+ Select members’**

[![21-add-roles-member-select.png](https://i.postimg.cc/zGDpHBcF/21-add-roles-member-select.png)](https://postimg.cc/rKb5v8cD)

4.- En la ventana abierta escribir nuestro usuario y selecionarlo

[![22-select-member.png](https://i.postimg.cc/qqVHpnQL/22-select-member.png)](https://postimg.cc/vgzNz4Jg)

Clic en **“select” —> “Review + assign”**

5.- Realizamos los siguientes pasos:

-  Ir a “Access control (IAM), luego **“Add”**, clic en **“Add role assignment”**.
-  En **“Job function roles”** seleccionar **“key vault Administrator”**, luego clic en**“Members”**.
- En **“Assign access to”** seleccionar **‘User, group, or service principal’**, luego en **“Members”** clic en **‘+ Select members’**.
-  En la ventana abierta escribir nuestro usuario y selecionarlo.
-  Clic en **“select” —> “Review + assign”**

[![50-2-Acces-control-IAM.png](https://i.postimg.cc/Z5DC9qC1/50-2-Acces-control-IAM.png)](https://postimg.cc/mPCbJB3w)

[![50-3-Add-role-assignment.png](https://i.postimg.cc/CKV9sGBD/50-3-Add-role-assignment.png)](https://postimg.cc/QF06XWqx)

# CREACIÓN DE SECRETS USANDO KEY VAULT

1.- Ir a nuestro recurso **“Key Vault”**, a la sección de Objetcs y clic en **“Acces configuration”**, verificamos que la opción **Azure role-based access control (recommended)** este habilitado.

[![11-Azure-acces-control.png](https://i.postimg.cc/66fZk843/11-Azure-acces-control.png)](https://postimg.cc/NLLLTf4q)
 

2.- Ir a nuestro recurso **“Key Vault”**, a la sección de Objetcs y clic en **“Secrets”**, finalmente clic en **“Generate/Import”**

[![50-generete-key-vaul.png](https://i.postimg.cc/2SsY7FkF/50-generete-key-vaul.png)](https://postimg.cc/gxH1Yh6n)

3.- Crear el secret para el **usuario** de conexión a Kafka

- **Name:**  UsuarioKafka
- **Secret vault: ** (copiamos el **key** que nos genero Kafka al momento de crear la APY )
- Click en **Create**

4.- Crear el secret para el **PasswordKafka** de conexión a Kafka

- **Name:**  PasswordKafka
- **Secret vault: ** (copiamos el **secret** que nos genero Kafka al momento de crear la APY )
- Click en **Create**

4.- Crear el secret para el **ServerKafka** de conexión a Kafka

- **Name:**  ServerKafka
- **Secret vault: ** (copiamos el **la cadena de conexión del servidor de kafka**
- Click en **Create**

5.- Crear el secret para el **ServerMongoDB** de conexión a Kafka

- **Name:**  ServerMongoDB
- **Secret vault: ** (copiamos el **la cadena de conexión del servidor de MongoDB**
- Click en **Create**

[![15-secrets.png](https://i.postimg.cc/Yq4cR6BF/15-secrets.png)](https://postimg.cc/TpXHYWw2)

6.- Verificamos que la opción **Vault access control policy** este habilitado.

[![50-1-Acces-configuration-key-vaul.png](https://i.postimg.cc/pd1p3Lf2/50-1-Acces-configuration-key-vaul.png)](https://postimg.cc/B8HSKs7y)


# CREACIÓN AZURE DATABRICKS

1.- Utilizar el grupo de recurso creado previamente

- **Nombre grupo de recurso: **rg-datapath-synapse-002

2.-  Ingresar al grupo de recursos creado, y en la parte superior seleccionar **“Create”**

[![2-ingresar-RG.png](https://i.postimg.cc/L4T7gH6y/2-ingresar-RG.png)](https://postimg.cc/NLKbWvXT)

3.- En el Marketplace escribir **“azure databricks”** y seleccionar el recurso, luego clic en **“create”**

[![34-azure-data-bricks.png](https://i.postimg.cc/VvXMbLDB/34-azure-data-bricks.png)](https://postimg.cc/T5YhzG8p)

4.- Configurar el workspace en **“basics”:**

- **a. Resource group:** rg-datapath-synapse-002

- **b. Workspace name:** dbw-datapath-etl

- **c. Region:** East US 2

- **d. Pricing Tier:** Trial (Premium – 14 Days Free DBUs)


[![35-create-azure-data-bricks.png](https://i.postimg.cc/wTvgvbM3/35-create-azure-data-bricks.png)](https://postimg.cc/6TDDbcdN)

Clic en **“Review + create” —> “Create” —> “Go to Resource”**

4.- Una vez creado el workspace, clic en **“Launch Workspace”**

[![36-clic-launch-workspace.png](https://i.postimg.cc/C1XXxJQn/36-clic-launch-workspace.png)](https://postimg.cc/62VYHVwB)

5.- Finalmente se abrirá una nueva ventana en el navegador con el espacio de trabajo de Databricks

[![37-workspace-databricks.png](https://i.postimg.cc/FRCmgjnh/37-workspace-databricks.png)](https://postimg.cc/9R9vcqhk)

# CREACIÓN CLUSTER AZURE DATABRICKS

1.- En el espacio de trabajo de Databricks ir a la parte izquierda y
seleccionar **“Compute”**

[![37-workspace-databricks.png](https://i.postimg.cc/FRCmgjnh/37-workspace-databricks.png)](https://postimg.cc/9R9vcqhk)

2.- En la parte derecha del panel dar clic en **“Create Compute”**

[![38-create-compute.png](https://i.postimg.cc/L8n3Df4N/38-create-compute.png)](https://postimg.cc/4YRcNY99)


3.- Configurar el computo de nuestro cluster

- **Nombre Cluster:** Datapath_Cluster

- **Policy:** Unrestricted

- **Single node**

- **Access Mode:** No isolation shared

- **Databricks runtime version:** Runtime: 10.4 LTS (Scala 2.12,
- Spark 3.2.1)

- **NO seleccionar** “use photon Acceleration”

- **Node Type:** Por defecto

- **Terminate after** “30” minutes

[![39-configuracion-cluster.png](https://i.postimg.cc/j55xns4w/39-configuracion-cluster.png)](https://postimg.cc/zLsZ9YM8)

Clic en **“Create Compute”** 

Tiempo estimado: 3-5 minutos


# CREATE A SECRET SCOPES CONNECTED TO AZURE KEY VAULT

1.- Ir al workspace, luego a nuestro notebook de **“Mount Storage”**, finalmente modificar la url del workspace desde el **“#”**

**Antes:**
[![45-url-Mount-Storage.png](https://i.postimg.cc/JhDyGnsG/45-url-Mount-Storage.png)](https://postimg.cc/xkQjZ9y2)

reemplazar en parte de la url: #secrets/createScope

**Después:**

[![45-url-Mount-Storage-DESPUES.png](https://i.postimg.cc/tC44sTfp/45-url-Mount-Storage-DESPUES.png)](https://postimg.cc/LhGp7H2b)

**Sintaxis:**

https://<your_azure_databricks_url>#secrets/createScope

[![46-create-scope.png](https://i.postimg.cc/pdZS523M/46-create-scope.png)](https://postimg.cc/LqJDrMHx)

2.- Una vez realizado el paso anterior, nuestra ventana cambiará al Homepage **“Create Secret Scope”**

Creación de Secrets 
- **Scope name:** “sc-adls”

- **Manage principal:** “All Users”

**CONFIGURACIÓN AZURE KEY VAULT**

3.- Ir a nuestro recurso **“Key Vault”**

- **Ir a properties**

- **copiar y pegar DNS = Vault URI**

- **Copiar y pegar Resource ID**

[![47-key-vault.png](https://i.postimg.cc/Px6rynpB/47-key-vault.png)](https://postimg.cc/1nqQRTKH)

4.- Ir a nuestra ventana de **Secret Scope **y pegar los valores previamente copiados, finalmente clic en **“Create”**

[![48-create-scope-options.png](https://i.postimg.cc/yd60WBC5/48-create-scope-options.png)](https://postimg.cc/67S81k3L)

5. Aplicamos cambios, clic en **“ok”**

[![49-clic-ok.png](https://i.postimg.cc/3rVtRLnB/49-clic-ok.png)](https://postimg.cc/F1byCxtd)

# Importar Notebook a nuestro Folder en Databricks

1.- Explorar nuestro **workspace**, para crear Folder, notebooks etc.

[![40-create-folder.png](https://i.postimg.cc/Y0GvJPMh/40-create-folder.png)](https://postimg.cc/GTR3DqBC)

2.- Creamos un folder con los siguientes datos:

- **New Folder:** Proyectos_Databricks

[![10-create-folder.png](https://i.postimg.cc/QM9PKd5J/10-create-folder.png)](https://postimg.cc/67KzDwM8)

3.- Importamos los archivos que se encuentran en la carpeta **notebook** dentro de este repositorio de github, los cuales contienen los scripts:

- **Create_Database:** Configuración inicial de la base de datos y las tablas en Databricks.
- **Producer_Messager**: Generación de datos ficticios y envío a un tópico de Kafka.
- **Streaming_Messages:** Consumo de datos en tiempo real desde Kafka y almacenamiento en tablas Delta.

[![18-import-notebook.png](https://i.postimg.cc/pdFmqM1Z/18-import-notebook.png)](https://postimg.cc/wt9xjZzy)

4.- Instalamos librerías para nuestro procesos de **Streaming_Messages**.

Nos dirigimos a la opción **Compute**, seleccionamos nuestro cluster **Datapath_Cluster** y nos dirigimos a la opción **Libraries**, damos clic en la botón **Install new**

[![16-libraries.png](https://i.postimg.cc/NFjWDBfC/16-libraries.png)](https://postimg.cc/5YD7Ncz8)

Nos dirigimos a la opción Maven y copiamos el siguiente código y le damos clic al botón Install

	org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3

[![17-maven.png](https://i.postimg.cc/9QsCfRFm/17-maven.png)](https://postimg.cc/PCbB3qw9)

# Configuración de Workflows

Nos dirigimos a la opción **Workflows** damos clic en el botón Create Job

[![19-create-Job.png](https://i.postimg.cc/fbPxbhVn/19-create-Job.png)](https://postimg.cc/5HwHPT5n)

Editamos el nombre del job y lo renombramos por **job_streaming*

Nos dirigimos a la pestaña **Task** e ingresamos los siguientes datos de acuerdo a las opciones:

- **Task name:** Create_Database
- **Type:** Notebook
- **Source:** Workspace
- **Path:** Nos dirigimos al Notebook **Create_Database** que tenemos importado dentro de nuestro **Workspace**
- **Cluster:** Seleccionamos el cluster que tenemos creado **Datapah_cluster**
- Clic en **Create Task**

[![20-task-database.png](https://i.postimg.cc/nzk19Gnq/20-task-database.png)](https://postimg.cc/XGr5TFGv)

Realizamos el mismo procedimientos para nuestros Notebooks **Producer_Messager y Streaming_Messages** quedando nuestros **tasks** de la siguiente manera:

Para estos **tasks** en la sección **Depends on**, seleccionamos que dependan del **task Create_Database**, pues primero debe crear la base de datos para luego los siguientes procesos ejecutarse en paralelo.

[![21-task-producer-stream.png](https://i.postimg.cc/JzTbczXB/21-task-producer-stream.png)](https://postimg.cc/jD7wRKZs)

## Configuración de ejecución de nuestro job

En la sección derecha de nuestro job, búscamos la opción **Schedules & Triggers** damos clic en el botón **Add Triggers**, seleccionando la opción **Scheduled**.

[![job-scheduller.png](https://i.postimg.cc/6p37wsjr/job-scheduller.png)](https://postimg.cc/56h9qrMy)

Quedando de la siguiente manera.

[![22-job-schedule.png](https://i.postimg.cc/mktqBg9x/22-job-schedule.png)](https://postimg.cc/2LNTxmTT)

### Agregar notificación en caso de error

En la sección derecha de nuestro job, búscamos la opción **Job notifications** y damos clic en el botón **Edit notifications**

[![23-agregar-notificacion.png](https://i.postimg.cc/wvcJMXhL/23-agregar-notificacion.png)](https://postimg.cc/DJ0mYbpw)

En la casilla **Destination** agregamos el correo al que deseamos se envíe el correo en caso de fallo al momento de ejecutar el job, seleccionando el check de **Failure**, habilitar los check de **Mute notifications for skipped runs** y **Mute notifications for canceled runs**, damos clic en el botón save.

[![24-job-notifications.png](https://i.postimg.cc/X7vHMqbQ/24-job-notifications.png)](https://postimg.cc/bdKHkzdt)

Ejecutamos el job_dando clic al botón **Run now**

[![25-runjob.png](https://i.postimg.cc/WbvxqT0v/25-runjob.png)](https://postimg.cc/s1TTtFn6)

[![26-job-succefull.png](https://i.postimg.cc/TPMtFcQX/26-job-succefull.png)](https://postimg.cc/ZWHPyp1f)

[![27-job-sucefull-2.png](https://i.postimg.cc/LXZjzJr6/27-job-sucefull-2.png)](https://postimg.cc/wygtzjqS)

## Verificación de carga de datos de tablas Delta Lake

Importamos el Notebook **Querys_Sql** que se encuentra en este repositorio de Github, en este Notebook **Querys_Sql** encontraremos diferentes querys sql que nos permitiran consultar la carga de nuestras tablas **Delta Lake** que hemos utilizado en nuestro proceso, en el Notebook encontramos otras consultas adicionales a las que mostramos líneas abajo.

Consulta **SQL** a la tabla **db_alumnos.tbalumnos**, información que se carga en streaming.

	select * from db_alumnos.tbalumnos order by fechaRegistroKafka desc limit 5 ;

[![28-Consulta-tabla-tbalumnos.png](https://i.postimg.cc/G3DGSs9k/28-Consulta-tabla-tbalumnos.png)](https://postimg.cc/vDQ44TrB)

Consulta **SQL** a la tabla **db_alumnos.tbalumnos** con la propiedad **TABLE_CHANGES** que nos permitirá  detectar y responder a cambios en los datos de manera eficiente.

	select * from TABLE_CHANGES('db_alumnos.tbalumnos', 0);

[![29-consulta-changes-tbalumnos.png](https://i.postimg.cc/FRLdq2F9/29-consulta-changes-tbalumnos.png)](https://postimg.cc/8JTzfXr3)
