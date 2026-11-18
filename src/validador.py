"""
Módulo de QA de datos con Apache Spark y Python.
Verifica reglas de negocio base y avanzadas (reto autónomo) sobre transacciones.
"""

import os
import sys
from pathlib import Path

# En Windows: forzar a PySpark a usar el intérprete del entorno virtual (.venv)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

# Límite máximo permitido según regla de negocio R3
LIMITE_VALOR_MAX = 100_000_000

# Esquema explícito del DataFrame
SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("cliente_id", IntegerType(), True),
    StructField("valor", LongType(), True)
])


def crear_spark():
    """Inicializa la sesión de Spark en modo local utilizando 2 núcleos de CPU."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("Riwi-QA-Transacciones")
        .getOrCreate()
    )


# REGLAS DE NEGOCIO

def obtener_errores_valor(df):
    """Regla Base: Registros cuyo valor es nulo, cero o negativo."""
    return df.filter(col("valor").isNull() | (col("valor") <= 0))


def obtener_errores(df):
    """Alias compatible con la implementación inicial (Regla Base)."""
    return obtener_errores_valor(df)


def obtener_errores_cliente_nulo(df):
    """Regla R1: cliente_id no puede ser nulo."""
    return df.filter(col("cliente_id").isNull())


def obtener_errores_id_duplicado(df):
    """Regla R2: id de transacción no puede repetirse (retorna todas las filas duplicadas)."""
    ids_duplicados = df.groupBy("id").count().filter(col("count") > 1).select("id")
    return df.join(ids_duplicados, on="id", how="inner")


def obtener_errores_valor_excedido(df):
    """Regla R3: valor no puede superar el límite de 100,000,000."""
    return df.filter(col("valor") > LIMITE_VALOR_MAX)


def obtener_todas_filas_invalidas(df):
    """
    Retorna las filas únicas que incumplen AL MENOS una regla de calidad.
    Distingue entre filas inválidas y cantidad acumulada de errores.
    """
    ids_duplicados = df.groupBy("id").count().filter(col("count") > 1).select("id")

    # Reglas a nivel de fila individual
    condicion_filas = (
        col("valor").isNull()
        | (col("valor") <= 0)
        | (col("valor") > LIMITE_VALOR_MAX)
        | col("cliente_id").isNull()
    )

    filas_error_columna = df.filter(condicion_filas)
    filas_error_duplicado = df.join(ids_duplicados, on="id", how="inner").select(df.columns)

    return filas_error_columna.union(filas_error_duplicado).distinct()


def generar_resumen_calidad(df):
    """
    Regla R4: Genera un resumen con la cantidad de errores detectados por cada regla
    y el total de filas únicas inválidas encontradas.
    """
    cant_valor = obtener_errores_valor(df).count()
    cant_cliente_nulo = obtener_errores_cliente_nulo(df).count()
    cant_duplicados = obtener_errores_id_duplicado(df).count()
    cant_excedidos = obtener_errores_valor_excedido(df).count()
    total_filas_invalidas = obtener_todas_filas_invalidas(df).count()

    total_errores = cant_valor + cant_cliente_nulo + cant_duplicados + cant_excedidos

    return {
        "valor_nulo_o_negativo": cant_valor,
        "cliente_id_nulo": cant_cliente_nulo,
        "id_duplicado": cant_duplicados,
        "valor_excedido": cant_excedidos,
        "total_errores_acumulados": total_errores,
        "total_filas_invalidas": total_filas_invalidas
    }


# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

def procesar_archivo(spark, nombre_archivo):
    """Carga y valida un archivo CSV mostrando los resultados de calidad."""
    ruta = Path(__file__).resolve().parents[1] / "datos" / nombre_archivo

    if not ruta.exists():
        print(f"\n[AVISO] Archivo no encontrado: {nombre_archivo}")
        return

    print(f"\n{'=' * 20} PROCESANDO: {nombre_archivo} {'=' * 20}")
    df = (
        spark.read
        .option("header", True)
        .schema(SCHEMA)
        .csv(str(ruta))
    )

    print("\n--- DATOS ORIGINALES ---")
    df.show()

    resumen = generar_resumen_calidad(df)
    print("\n--- RESUMEN DE CONTROL DE CALIDAD (QA) ---")
    print(f"  • [Base] Valor nulo o <= 0:         {resumen['valor_nulo_o_negativo']}")
    print(f"  • [R1]   Cliente ID nulo:           {resumen['cliente_id_nulo']}")
    print(f"  • [R2]   Registros con ID duplicado:{resumen['id_duplicado']}")
    print(f"  • [R3]   Valor excede límite máx.:  {resumen['valor_excedido']}")
    print(f"  --------------------------------------------------")
    print(f"  • Total errores detectados:         {resumen['total_errores_acumulados']}")
    print(f"  • Total filas únicas inválidas:     {resumen['total_filas_invalidas']}")

    filas_invalidas = obtener_todas_filas_invalidas(df)
    if resumen["total_filas_invalidas"] > 0:
        print("\n--- FILAS CON INCUMPLIMIENTOS ---")
        filas_invalidas.show()
        print(f"RESULTADO: FAIL ({resumen['total_filas_invalidas']} filas inválidas)")
    else:
        print("\nRESULTADO: PASS (Archivo 100% conforme a las reglas de negocio)")


def main():
    spark = crear_spark()
    try:
        # 1. Validar dataset base del laboratorio
        procesar_archivo(spark, "transacciones.csv")

        # 2. Validar dataset del reto autónomo si existe
        procesar_archivo(spark, "transacciones_reto.csv")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
