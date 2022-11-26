import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as functions
from pyspark.sql.functions import regexp_replace, when, year, month, to_date, col
from pyspark.sql.types import StringType, IntegerType, FloatType
import re


def start_or_create_spark():
    from pyspark.sql import SparkSession
    spark = (SparkSession
             .builder
             .appName("Processamento de Dados de Gasolina no Brasil")
             .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar')
             .getOrCreate()
             )
    return spark


def rename_columns(dataframe):
    """
    Escreva uma função que receba o Dataframe Spark e realize o rename de colunas acordo
    com a documentação do BigQuery: https://cloud.google.com/bigquery/docs/schemas#:~:text=description%20and%20mode.-,Column%20names,name%20length%20is%20300%20characters.
    e retorne o Dataframe com as colunas renomeadas.
    Tip: withColumnRenamed
    """
    dataframe = dataframe.select([functions.col(x).alias(x.lower()) for x in dataframe.columns])
    dataframe = dataframe.select([functions.col(col).alias(re.sub(" -", "", col)) for col in dataframe.columns])
    dataframe = dataframe.select([functions.col(col).alias(re.sub(" ", "_", col)) for col in dataframe.columns])

    return dataframe


def add_year(dataframe, coluna):
    """
    Parametros: dataframe, coluna

    Escreva uma função que receba um Dataframe Spark e o nome da coluna que será baseada para criar
    uma coluna `year` no Dataframe com os dados lidos do GCS.
    O resultado da coluna deverá ser o Ano.
    E retorne o dataframe com os dados e a nova coluna criada.
    """
    dataframe = dataframe.withColumn("data", to_date(col(coluna), "dd/MM/yyyy"))
    dataframe = dataframe.withColumn("ano", year(col("data")))

    return dataframe


def add_semestre(dataframe, coluna):
    """
    Parametros: dataframe, coluna

    Escreva uma função que receba um Dataframe Spark e o nome da coluna que será baseada para criar
    uma coluna `semestre` no Dataframe com os dados lidos do GCS. O resultado deverá ser 1 ou 2.
    E retorne o dataframe com os dados e a nova coluna criada.
    """

    dataframe = dataframe.withColumn('semestre', month(coluna))
    dataframe = dataframe.withColumn('semestre', when(col("semestre") < 7, 1)
                                     .when(col("semestre") >= 7, 2))

    return dataframe


def add_filename_input(dataframe):
    """
    Parametros: dataframe

    Escreva uma função que receba um Dataframe Spark que crie uma coluna `input_file_name` que será baseada no nome do arquivo lido.
    E retorne o dataframe com os dados e a nova coluna criada.
    """

    dataframe = dataframe.withColumn("input_file_name", functions.input_file_name())

    return dataframe


def put_file_gcs(dataframe, path_output, formato):
    """
    :param path_output: path para save dos dados
    :param dataframe: conjunto de dados a serem salvos
    :param formato: tipo de arquivo a ser salvo
    :return: None

    Escreva uma função que salve os dados no GCS, utilizando o metodo write do Dataframe Spark.
    Tip:
    """
    dataframe.repartition(1).write.format(formato).mode("overwrite").save(path_output)

    return None


def write_bigquery(dataframe, bq_dataset, bq_table, gcs_tmp_bucket):
    """
    Crie uma função que receba os parametros:
    :param dataframe: conjunto de dados a serem salvos
    :param tabela: Tabela do BigQuery que será salvo os dados. Ex: dataset.tabela_exemplo
    :param temporaryGcsBucket: Bucket temporário para salvar area de staging do BigQuery.

    E escreva dentro do BigQuery.
    Utilize o material de referencia:
    https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#running_the_code
    """

    # spark.conf.set('temporaryGcsBucket', gcs_tmp_bucket)
    dataframe.write \
        .format("bigquery") \
        .option("table", "{}.{}".format(bq_dataset, bq_table)) \
        .option("temporaryGcsBucket", gcs_tmp_bucket) \
        .mode('append') \
        .save()

    return None


def main(path_input, path_output, file_format, bq_dataset, bq_table, gcs_tmp_bucket):
    try:
        """
        Crie uma função main que receba como parametro:
        path_input: Caminho dos dados no GCS gerados pela API coletora. Ex: gs://bucket_name/file_name
        path_output: Caminho de onde será salvo os dados processados. Ex: gs://bucket_name_2/file_name
        formato_file_save: Formato de arquivo a ser salvo no path_output. Ex: PARQUET
        tabela_bq: Tabela do BigQuery que será salvo os dados. Ex: dataset.tabela_exemplo


        1 - Faça a leitura dos dados de acordo com o path_input informado
        2 - Realize o rename de colunas do arquivo, respeitando os padroes do BigQuery
        3 - Adicione uma coluna de Ano, baseado na coluna `Data da Coleta`
        4 - Adicione uma coluna de Semestre, baseado na coluna de `Data da Coleta`
        5 - Adicione uma coluna Filename. Tip: pyspark.sql.functions.input_file_name
        6 - Faça o parse dos dados lidos de acordo com a tabela no BigQuery
        7 - Escreva os dados no Bucket GCS, no caminho informado `path_output` 
            no formato especificado no atributo `formato_file_save`.
        8 - Escreva os dados no BigQuery de acordo com a tabela especificada no atributo `tabela_bq`
        """
        spark = start_or_create_spark()
        df = spark.read.format('csv').option("header", "true").option('delimiter', ';').load(path_input)
        df = rename_columns(df)
        df = add_year(df, "data_da_coleta")
        df = add_semestre(df, "data")
        df = add_filename_input(df)
        df = df.withColumn("numero_rua", col("numero_rua").cast(IntegerType())) \
            .withColumn("ano", col("ano").cast(StringType())) \
            .withColumn("semestre", col("semestre").cast(StringType())) \
            .withColumn('valor_de_venda', regexp_replace('valor_de_venda', ',', '.').cast(FloatType())) \
            .withColumn('valor_de_compra', regexp_replace('valor_de_compra', ',', '.').cast(FloatType()))
        put_file_gcs(df, path_output, file_format)
        write_bigquery(df, bq_dataset, bq_table, gcs_tmp_bucket)

        return df
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--path_input',
        type=str,
        dest='path_input',
        required=True,
        help='URI of the GCS bucket, for example, gs://bucket_name/file_name')

    parser.add_argument(
        '--path_output',
        type=str,
        dest='path_output',
        required=True,
        help='URI of the GCS bucket, for example, gs://bucket_name/file_name')

    parser.add_argument(
        '--file_format',
        type=str,
        dest='file_format',
        required=True,
        help='Type format save file')

    parser.add_argument(
        '--bq_dataset',
        type=str,
        dest='bq_dataset',
        required=True,
        help='Dataset do BQ')

    parser.add_argument(
        '--table_bq',
        type=str,
        dest='table_bq',
        required=True,
        help='Tabela do BigQuery Destino')

    known_args, pipeline_args = parser.parse_known_args()

    main(path_input=known_args.path_input,
         path_output=known_args.path_output,
         file_format=known_args.file_format,
         bq_dataset="gasolina_brasil",
         bq_table="tb_historico_combustivel_brasil",
         gcs_tmp_bucket="data-pipeline-stack-combustiveis-brasil-pyspark-code"
         )
