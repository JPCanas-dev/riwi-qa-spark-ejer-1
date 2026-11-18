"""
Pruebas automatizadas unitarias con Pytest para el validador de datos PySpark.
Cubre las pruebas base del laboratorio y las reglas del Reto Autónomo (R1 a R5).
"""

import pytest

from src.validador import (
    SCHEMA,
    LIMITE_VALOR_MAX,
    crear_spark,
    obtener_errores,
    obtener_errores_cliente_nulo,
    obtener_errores_id_duplicado,
    obtener_errores_valor_excedido,
    obtener_todas_filas_invalidas,
    generar_resumen_calidad,
)


@pytest.fixture(scope="module")
def spark():
    """Comparte una única sesión de Spark para toda la suite de pruebas."""
    sesion = crear_spark()
    yield sesion
    sesion.stop()


# ============================================================================
# PRUEBAS BASE DEL LABORATORIO
# ============================================================================

def test_datos_validos_no_generan_errores(spark):
    """Verifica que un DataFrame con registros 100% conformes retorne cero errores."""
    datos = [(1, 101, 50000), (2, 102, 10000), (3, 103, 75000)]
    df = spark.createDataFrame(datos, SCHEMA)
    assert obtener_errores(df).count() == 0


def test_datos_invalidos_son_detectados(spark):
    """Verifica detección de valores negativos, nulos y cero en la columna valor."""
    datos = [
        (1, 101, 50000),   # Válido
        (2, 102, -20000),  # Inválido: negativo
        (3, 103, None),    # Inválido: nulo
        (4, 104, 0),       # Inválido: cero
    ]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores(df)

    assert errores.count() == 3
    ids = {fila["id"] for fila in errores.select("id").collect()}
    assert ids == {2, 3, 4}


# ============================================================================
# PRUEBAS DEL RETO AUTÓNOMO (R5)
# ============================================================================

def test_r1_cliente_id_nulo_es_detectado(spark):
    """R1: Verifica que se identifiquen transacciones con cliente_id nulo."""
    datos = [
        (1, 101, 50000),  # Válido
        (2, None, 20000), # Inválido: cliente_id es None
        (3, 103, 75000),  # Válido
    ]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores_cliente_nulo(df)

    assert errores.count() == 1
    ids = [fila["id"] for fila in errores.select("id").collect()]
    assert ids == [2]


def test_r2_id_duplicado_es_detectado(spark):
    """R2: Verifica que se detecten las transacciones cuyos IDs se encuentren repetidos."""
    datos = [
        (1, 101, 50000),  # Válido (ID único)
        (2, 102, 20000),  # Inválido: ID 2 repetido (aparición 1)
        (2, 103, 35000),  # Inválido: ID 2 repetido (aparición 2)
        (3, 104, 40000),  # Válido (ID único)
    ]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores_id_duplicado(df)

    # Debe retornar las 2 filas involucradas en la colisión de ID
    assert errores.count() == 2
    ids = {fila["id"] for fila in errores.select("id").collect()}
    assert ids == {2}


def test_r3_valor_excedido_es_detectado(spark):
    """R3: Verifica que transacciones con valor superior a 100,000,000 sean detectadas."""
    datos = [
        (1, 101, 50000),                  # Válido
        (2, 102, LIMITE_VALOR_MAX),       # Válido (en el límite exacto)
        (3, 103, LIMITE_VALOR_MAX + 1),   # Inválido: supera el límite
    ]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores_valor_excedido(df)

    assert errores.count() == 1
    ids = [fila["id"] for fila in errores.select("id").collect()]
    assert ids == [3]


def test_r4_resumen_calidad_cuenta_errores_y_filas_invalidas(spark):
    """
    R4: Verifica que el resumen distinga entre cantidad acumulada de errores
    y la cantidad total de filas únicas inválidas cuando un registro tiene múltiples fallas.
    """
    datos = [
        (1, 101, 50000),               # Válido (0 errores)
        (2, None, -5000),              # Inválido con 2 errores a la vez (cliente nulo y valor negativo)
        (3, 103, LIMITE_VALOR_MAX + 10) # Inválido con 1 error (valor excedido)
    ]
    df = spark.createDataFrame(datos, SCHEMA)
    resumen = generar_resumen_calidad(df)

    # Verificación por regla individual
    assert resumen["valor_nulo_o_negativo"] == 1
    assert resumen["cliente_id_nulo"] == 1
    assert resumen["valor_excedido"] == 1
    assert resumen["id_duplicado"] == 0

    # Total de errores acumulados: 1 + 1 + 1 = 3 errores
    assert resumen["total_errores_acumulados"] == 3

    # Total de filas inválidas únicas: fila 2 y fila 3 = 2 filas
    assert resumen["total_filas_invalidas"] == 2
