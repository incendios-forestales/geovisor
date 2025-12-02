# =============================================================================
# PRE-PROCESAMIENTO DE DATOS
# Incendios Forestales en Costa Rica
# =============================================================================
# 
# INSTRUCCIONES:
# 1. Este script debe ejecutarse UNA SOLA VEZ.
# 2. Los archivos originales deben estar en la carpeta "datos/".
# 3. El script creará archivos .rds optimizados en "datos/procesados/".
# 4. La carpeta "datos/procesados/" debe subirse a shinyapps.io.
#
# =============================================================================

library(sf)
library(dplyr)

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------

# Carpetas
CARPETA_ORIGEN    <- "datos"
CARPETA_DESTINO   <- "datos/procesados"

# Parámetros de filtrado inicial
CONFIANZA_MINIMA_ABSOLUTA <- 80
FECHA_MINIMA <- as.Date("2001-01-01")
FECHA_MAXIMA <- as.Date("2024-12-31")

# Tolerancia para simplificación de geometrías (en metros)
# Valores más altos = geometrías más simples = carga más rápida
# Recomendado: 50-200 metros para visualización web
TOLERANCIA_SIMPLIFICACION <- 100

# -----------------------------------------------------------------------------
# CREAR CARPETA DE DESTINO
# -----------------------------------------------------------------------------

if (!dir.exists(CARPETA_DESTINO)) {
  dir.create(CARPETA_DESTINO, recursive = TRUE)
  message("Carpeta creada: ", CARPETA_DESTINO)
}

# -----------------------------------------------------------------------------
# CARGAR ÁREAS DE CONSERVACIÓN (AC) - SIN SIMPLIFICAR
# -----------------------------------------------------------------------------

message("\n", strrep("=", 60))
message("Cargando Áreas de Conservación...")
message(strrep("=", 60))

ac <- st_read(
  file.path(CARPETA_ORIGEN, "areas-conservacion.gpkg"),
  quiet = TRUE
) |>
  st_make_valid()

message("- Registros: ", nrow(ac))
message("- Tamaño: ", format(object.size(ac), units = "MB"))

# -----------------------------------------------------------------------------
# CARGAR ÁREAS SILVESTRES PROTEGIDAS (ASP) - SIN SIMPLIFICAR
# -----------------------------------------------------------------------------

message("\n", strrep("=", 60))
message("Cargando Áreas Silvestres Protegidas...")
message(strrep("=", 60))

asp <- st_read(
  file.path(CARPETA_ORIGEN, "asp.gpkg"),
  quiet = TRUE
) |>
  st_make_valid() |>
  group_by(siglas_cat, nombre_asp) |>
  summarize(
    do_union = TRUE,
    .groups = "drop"
  ) |>
  mutate(
    nombre_asp = paste(siglas_cat, nombre_asp),
    nombre_asp = case_when(
      nombre_asp == "HH Estero Puntarenas y Manglares Asociados" ~ "HH Estero Puntarenas",
      nombre_asp == "HH Humedal lacustrino Rio Canas" ~ "HH Rio Canas",
      .default = nombre_asp
    )
  ) |>
  select(nombre_asp) |>
  st_cast("MULTIPOLYGON")

message("- Registros: ", nrow(asp))
message("- Tamaño: ", format(object.size(asp), units = "MB"))

# -----------------------------------------------------------------------------
# PROCESAR INCENDIOS CON SPATIAL JOIN (usando geometrías completas)
# -----------------------------------------------------------------------------

message("\n", strrep("=", 60))
message("Procesando Incendios...")
message(strrep("=", 60))

incendios_raw <- st_read(
  file.path(CARPETA_ORIGEN, "incendios_clima_cr_2001-2024.gpkg"),
  quiet = TRUE
)

message("- Registros originales: ", nrow(incendios_raw))
message("- Tamaño original: ", format(object.size(incendios_raw), units = "MB"))

# Filtrar y convertir fecha
incendios <- incendios_raw |>
  mutate(acq_date = as.Date(acq_date)) |>
  filter(
    confidence >= CONFIANZA_MINIMA_ABSOLUTA,
    acq_date >= FECHA_MINIMA,
    acq_date <= FECHA_MAXIMA
  )

message("- Registros después de filtrar: ", nrow(incendios))

# SPATIAL JOIN con geometrías COMPLETAS para máxima precisión
message("\n- Ejecutando spatial join con AC (geometría completa)...")
t1 <- Sys.time()

incendios <- incendios |>
  st_join(
    select(ac, nombre_ac),
    join = st_intersects
  )

message("  Completado en ", round(difftime(Sys.time(), t1, units = "secs"), 1), " segundos")

message("- Ejecutando spatial join con ASP (geometría completa)...")
t2 <- Sys.time()

incendios <- incendios |>
  st_join(
    select(asp, nombre_asp),
    join = st_intersects
  )

message("  Completado en ", round(difftime(Sys.time(), t2, units = "secs"), 1), " segundos")

# Seleccionar solo columnas necesarias para reducir tamaño
incendios <- incendios |>
  select(
    nombre_ac, nombre_asp, acq_date, acq_time,
    frp, brightness, confidence,
    TEMPERATURE_2M_ACQ_DATE_MEAN, PRECIPITATION_ACQ_DATE_SUM,
    X10M_WIND_SPEED_ACQ_DATE_MEAN, X10M_WIND_DIRECTION_ACQ_DATE_MEAN
  )

message("- Tamaño final: ", format(object.size(incendios), units = "MB"))

# Guardar incendios
saveRDS(incendios, file.path(CARPETA_DESTINO, "incendios.rds"))
message("- Guardado: incendios.rds")

# -----------------------------------------------------------------------------
# SIMPLIFICAR GEOMETRÍAS DE AC Y ASP (después del spatial join)
# -----------------------------------------------------------------------------

message("\n", strrep("=", 60))
message("Simplificando geometrías para visualización...")
message(strrep("=", 60))

# Simplificar AC
message("- Simplificando AC...")
ac_simple <- ac |>
  st_simplify(dTolerance = TOLERANCIA_SIMPLIFICACION, preserveTopology = TRUE) |>
  st_make_valid()

message("  Tamaño original: ", format(object.size(ac), units = "MB"))
message("  Tamaño simplificado: ", format(object.size(ac_simple), units = "MB"))

saveRDS(ac_simple, file.path(CARPETA_DESTINO, "ac.rds"))
message("  Guardado: ac.rds")

# Simplificar ASP
message("- Simplificando ASP...")
asp_simple <- asp |>
  st_simplify(dTolerance = TOLERANCIA_SIMPLIFICACION, preserveTopology = TRUE) |>
  st_make_valid()

message("  Tamaño original: ", format(object.size(asp), units = "MB"))
message("  Tamaño simplificado: ", format(object.size(asp_simple), units = "MB"))

saveRDS(asp_simple, file.path(CARPETA_DESTINO, "asp.rds"))
message("  Guardado: asp.rds")

# -----------------------------------------------------------------------------
# CREAR LISTAS PARA SELECTORES
# -----------------------------------------------------------------------------

message("\n", strrep("=", 60))
message("Creando listas para selectores...")
message(strrep("=", 60))

listas <- list(
  ac = c("Todas", sort(unique(na.omit(incendios$nombre_ac)))),
  asp = c("Todas", sort(unique(na.omit(incendios$nombre_asp)))),
  confianza_min = min(incendios$confidence, na.rm = TRUE),
  confianza_max = max(incendios$confidence, na.rm = TRUE),
  frp_min = min(incendios$frp, na.rm = TRUE),
  frp_max = max(incendios$frp, na.rm = TRUE),
  fecha_min = min(incendios$acq_date, na.rm = TRUE),
  fecha_max = max(incendios$acq_date, na.rm = TRUE)
)

saveRDS(listas, file.path(CARPETA_DESTINO, "listas.rds"))
message("- Guardado: listas.rds")

# -----------------------------------------------------------------------------
# RESUMEN FINAL
# -----------------------------------------------------------------------------

message("\n", strrep("=", 60))
message("RESUMEN DE ARCHIVOS GENERADOS")
message(strrep("=", 60))

archivos <- list.files(CARPETA_DESTINO, pattern = "\\.rds$", full.names = TRUE)
for (archivo in archivos) {
  info <- file.info(archivo)
  message(sprintf("  %-25s %s %s", 
                  basename(archivo), 
                  format(info$size, big.mark = ","), " bytes"))
}

total_size <- sum(file.info(archivos)$size)
message(strrep("-", 60))
message(sprintf("  TOTAL: %s", format(structure(total_size, class = "object_size"), units = "MB")))

message("\n¡Pre-procesamiento completado!")
message("Ahora puede usarse el dashboard optimizado con estos datos.")
message("\nPróximos pasos:")
message("1. Verificar que los archivos en 'datos/procesados/' se crearon correctamente.")
message("2. Usarlos en 'geovisor.qmd'.")
message("3. Desplegar en shinyapps.io incluyendo la carpeta 'datos/procesados/'.")
