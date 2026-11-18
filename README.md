# QA de Datos con Python y Apache Spark (Windows)

Laboratorio de control de calidad de datos, detección de anomalías en transacciones financieras y automatización de pruebas con Pytest utilizando **Apache Spark (PySpark 3.5.6)** y **Python 3.11** en Windows.

---

## 1. Estructura del Proyecto

```text
riwi-qa-spark/
├── .venv/                      # Entorno virtual local con Python 3.11 (ignorado por Git)
├── datos/
│   ├── transacciones.csv       # Dataset base con errores sembrados
│   ├── transacciones_validas.csv # Dataset 100% conforme
│   └── transacciones_reto.csv  # Dataset con casos para el reto autónomo
├── src/
│   ├── __init__.py
│   └── validador.py            # Motor de validación de calidad y reglas de negocio
├── tests/
│   └── test_validador.py       # Pruebas automatizadas unitarias con Pytest (R5)
├── requirements.txt            # Dependencias del proyecto
├── .gitignore                  # Exclusiones para control de versiones
└── README.md                   # Documentación, ejecución y reflexiones
```

---

## 2. Reglas de Negocio Implementadas

| Identificador | Regla | Condición Técnica | Impacto QA |
|---|---|---|---|
| **Base** | Valor obligatorio y positivo | `col('valor').isNull() \| (col('valor') <= 0)` | Evita transacciones con montos negativos o en blanco. |
| **R1** | Cliente obligatorio | `col('cliente_id').isNull()` | Detecta transacciones huérfanas sin titular. |
| **R2** | ID de transacción único | `groupBy('id').count() > 1` | Detecta duplicidad e inconsistencia en la clave primaria. |
| **R3** | Límite máximo de valor | `col('valor') > 100000000` | Alerta sobre montos sospechosos o que exceden topes legales. |
| **R4** | Resumen consolidado | Función `generar_resumen_calidad()` | Distingue entre total de fallas y cantidad de filas afectadas. |
| **R5** | Pruebas automatizadas | Suite en `tests/test_validador.py` | Asegura la confiabilidad del motor de reglas ante cambios. |

---

## 3. Guía de Ejecución en Windows (PowerShell)

### Paso 1. Ubicarse en el proyecto y activar el entorno
Abre PowerShell y colócate en la raíz del proyecto:

```powershell
cd D:\Big-data-spark\01-Talleres\01-Actividad-inicial\riwi-qa-spark
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force
.\.venv\Scripts\Activate.ps1
```

*(Debe aparecer el prefijo `(.venv)` al inicio de la línea de comandos).*

### Paso 2. Ejecutar el validador de datos
```powershell
python -m src.validador
```

### Paso 3. Ejecutar las pruebas automatizadas
```powershell
python -m pytest -v
```

---

## 4. Resultados de Ejecución

### Salida de `python -m src.validador`
```text
==================== PROCESANDO: transacciones.csv ====================
--- DATOS ORIGINALES ---
+---+----------+------+
| id|cliente_id| valor|
+---+----------+------+
|  1|       101| 50000|
|  2|       102|-20000|
|  3|       103|150000|
|  4|       104|  NULL|
|  5|       105|     0|
|  6|       106| 85000|
|  7|       107| 70000|
+---+----------+------+

--- RESUMEN DE CONTROL DE CALIDAD (QA) ---
  • [Base] Valor nulo o <= 0:         3
  • [R1]   Cliente ID nulo:           0
  • [R2]   Registros con ID duplicado:0
  • [R3]   Valor excede límite máx.:  0
  --------------------------------------------------
  • Total errores detectados:         3
  • Total filas únicas inválidas:     3

RESULTADO: FAIL (3 filas inválidas detectadas)

==================== PROCESANDO: transacciones_reto.csv ====================
--- DATOS ORIGINALES ---
+---+----------+---------+
| id|cliente_id|    valor|
+---+----------+---------+
|  1|       101|    50000|
|  2|       102|    20000|
|  2|       103|    35000|
|  3|      NULL|    80000|
|  4|       104|150000000|
|  5|       105|        0|
|  6|       106|     NULL|
+---+----------+---------+

--- RESUMEN DE CONTROL DE CALIDAD (QA) ---
  • [Base] Valor nulo o <= 0:         2
  • [R1]   Cliente ID nulo:           1
  • [R2]   Registros con ID duplicado:2
  • [R3]   Valor excede límite máx.:  1
  --------------------------------------------------
  • Total errores detectados:         6
  • Total filas únicas inválidas:     6

RESULTADO: FAIL (6 filas inválidas detectadas)
```

### Salida de `python -m pytest -v`
```text
tests/test_validador.py::test_datos_validos_no_generan_errores PASSED    [ 16%]
tests/test_validador.py::test_datos_invalidos_son_detectados PASSED      [ 33%]
tests/test_validador.py::test_r1_cliente_id_nulo_es_detectado PASSED     [ 50%]
tests/test_validador.py::test_r2_id_duplicado_es_detectado PASSED        [ 66%]
tests/test_validador.py::test_r3_valor_excedido_es_detectado PASSED      [ 83%]
tests/test_validador.py::test_r4_resumen_calidad_cuenta_errores_y_filas_invalidas PASSED [100%]

============================== 6 passed ==============================
```

---

## 5. Respuestas a las Preguntas de Reflexión (Sección 10)

### 1. ¿Qué diferencia hay entre un DataFrame de Spark y una lista de Python?
* **Lista de Python:** Es una estructura de datos nativa que reside íntegramente en la memoria RAM de un único hilo del computador local. Su procesamiento es secuencial (no distribuido) y si el conjunto de datos excede la memoria disponible del sistema, arroja un error crítico de `MemoryError`.
* **DataFrame de Spark:** Es una abstracción distribuida sobre un conjunto de datos particionado (RDD), evaluada de forma perezosa (*lazy evaluation*). Spark no carga todo el dataset en memoria de golpe; genera un plan de ejecución lógico y físico optimizado mediante el motor **Catalyst**, permitiendo procesar y distribuir terabytes de información entre decenas de nodos en un clúster.

### 2. ¿Por qué Spark no es necesario para siete registros?
Porque para siete registros, el costo de arranque (*overhead*) de inicializar la máquina virtual de Java (JVM), instanciar el contexto de Spark (`SparkSession`), planificar el grafo acíclico dirigido (DAG) y coordinar los hilos de ejecución toma entre 3 y 8 segundos, mientras que una simple lista por comprensión de Python o un script de pandas procesaría los mismos 7 registros en menos de un milisegundo. Spark está concebido para escenarios donde el volumen de datos supera la capacidad de una sola máquina o proceso estándar.

### 3. ¿Qué cambia cuando se deben validar decenas de millones de transacciones?
* **Límite de hardware:** Una máquina convencional colapsaría por falta de memoria RAM al intentar cargar 50 millones de transacciones en una lista o en pandas.
* **Paralelismo masivo:** Spark divide los datos en particiones y reparte el cómputo entre múltiples procesadores o servidores (*workers*), ejecutando las reglas de validación en paralelo.
* **Tolerancia a fallos:** Si un nodo de procesamiento falla a mitad de la validación, Spark recalcula únicamente la partición perdida gracias a su linaje de RDDs, sin tener que reiniciar todo el trabajo desde cero.

### 4. ¿Qué es una validación de datos y qué es una prueba automatizada del validador?
* **Validación de datos:** Es la ejecución de las reglas de negocio sobre los **datos reales o de producción** para comprobar si el archivo o la base de datos cumple con los estándares de calidad esperados (por ejemplo: si el archivo de hoy tiene registros con valores negativos). Su resultado es de negocio (`PASS` o `FAIL`).
* **Prueba automatizada del validador (Pytest):** Es una prueba sobre el **código del validador en sí mismo**. Utiliza datos sintéticos y controlados para verificar que las funciones de validación hacen exactamente lo que prometen (por ejemplo: confirmar que cuando le pasamos a propósito un registro negativo, el código efectivamente lo detecte). Si el validador arroja `FAIL: 3 errores`, pero Pytest arroja `PASSED`, significa que el código funciona a la perfección porque atrapó los errores que debía atrapar.

### 5. ¿Qué problemas puede causar una transacción duplicada en un pipeline financiero?
Una transacción financiera duplicada genera graves inconsistencias contables y operativas:
1. **Doble cobro o doble abono:** Afecta directamente el saldo real de un cliente o proveedor.
2. **Distorsión en métricas y balances:** Arruina los estados financieros diarios, reportes contables y modelos de conciliación bancaria.
3. **Pérdida de trazabilidad y riesgos regulatorios:** Puede activar falsas alertas de fraude o generar sanciones por parte de entes auditores y tributarios al reportar ingresos o egresos inexistentes.

### 6. ¿Qué controles aplicarías para comparar la cantidad de registros de entrada y salida de un ETL?
En un pipeline de datos profesional se aplican los siguientes controles de reconciliación:
1. **Control de recuento (Row Count Check):** Verificar que `Entrada == Salida + Registros_Rechazados`. Toda fila descartada debe quedar registrada en una tabla o archivo de cuarentena con su motivo.
2. **Suma de control (Checksum / Hash Diff):** Generar una suma o hash agregado sobre columnas financieras críticas (ej. la suma total de la columna `valor` antes y después de transformaciones sin agregación).
3. **Verificación de claves nulas y unicidad:** Validar que ninguna transformación haya introducido duplicados inesperados mediante `groupBy('id').count()`.
4. **Pruebas de integridad referencial:** Asegurar mediante joins de tipo *anti-join* (`left_anti`) que no existan IDs en la salida que no hayan existido en la fuente de entrada.
