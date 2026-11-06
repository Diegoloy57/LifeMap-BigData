
import os
import math
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import PyMongoError


# ============================================================
# LIFEMAP - API FLASK
# ============================================================

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "lifemap")
GIT_COMMIT = os.getenv("GIT_COMMIT", "unknown")

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=3000
)

db = client[DATABASE_NAME]

businesses_collection = db["businesses"]
closure_collection = db["closure_stats"]

MAX_LIMIT = 100
MAX_RADIUS = 50000


# ============================================================
# UTILIDADES
# ============================================================

def error_response(message, status=400):
    return jsonify({
        "status": "error",
        "error": message
    }), status


def serialize_document(document):
    document = dict(document)
    document.pop("_id", None)

    for key, value in document.items():
        if isinstance(value, datetime):
            document[key] = value.isoformat()

    return document


def parse_coordinates():
    try:
        lat = float(request.args["lat"])
        lon = float(request.args["lon"])
        radius = float(request.args.get("radius", 1000))
    except (KeyError, ValueError, TypeError):
        raise ValueError(
            "Debes proporcionar lat, lon y radius numéricos."
        )

    if not all(math.isfinite(v) for v in [lat, lon, radius]):
        raise ValueError("Las coordenadas y el radio deben ser finitos.")

    if not -90 <= lat <= 90:
        raise ValueError("La latitud debe estar entre -90 y 90.")

    if not -180 <= lon <= 180:
        raise ValueError("La longitud debe estar entre -180 y 180.")

    if not 0 < radius <= MAX_RADIUS:
        raise ValueError(
            f"El radio debe estar entre 0 y {MAX_RADIUS} metros."
        )

    return lat, lon, radius


def parse_limit(default=20):
    try:
        limit = int(request.args.get("limit", default))
    except (ValueError, TypeError):
        raise ValueError("limit debe ser un número entero.")

    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(
            f"limit debe estar entre 1 y {MAX_LIMIT}."
        )

    return limit


def validate_polygon(polygon):
    if not isinstance(polygon, dict):
        raise ValueError("polygon debe ser un objeto GeoJSON.")

    if polygon.get("type") != "Polygon":
        raise ValueError("polygon debe ser de tipo Polygon.")

    rings = polygon.get("coordinates")

    if not isinstance(rings, list) or not rings:
        raise ValueError("El polígono debe contener coordenadas.")

    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValueError(
                "Cada anillo debe tener al menos cuatro puntos."
            )

        if ring[0] != ring[-1]:
            raise ValueError(
                "El primer punto y el último deben ser iguales."
            )

        for point in ring:
            if not isinstance(point, (list, tuple)) or len(point) != 2:
                raise ValueError(
                    "Cada punto debe tener formato [longitud, latitud]."
                )

            lon, lat = point

            if (
                not isinstance(lon, (int, float))
                or isinstance(lon, bool)
                or not isinstance(lat, (int, float))
                or isinstance(lat, bool)
            ):
                raise ValueError("Las coordenadas deben ser numéricas.")

            if not math.isfinite(lon) or not math.isfinite(lat):
                raise ValueError("Las coordenadas deben ser finitas.")

            if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                raise ValueError("Coordenadas fuera del rango válido.")

    return polygon


# ============================================================
# 1. HEALTH CHECK
# GET /health
# ============================================================

@app.get("/health")
def health():
    try:
        client.admin.command("ping")

        mongo_status = "connected"

        businesses_count = businesses_collection.estimated_document_count()

        status = "ok"
        http_code = 200

    except PyMongoError:
        mongo_status = "disconnected"
        businesses_count = None
        status = "error"
        http_code = 503

    return jsonify({
        "service": "LifeMap API",
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git_commit": GIT_COMMIT,
        "database": {
            "name": DATABASE_NAME,
            "mongodb": mongo_status,
            "businesses_estimated": businesses_count
        },
        "components": {
            "flask": "running",
            "mongodb": mongo_status,
            "dask": "not_checked",
            "spark": "not_checked",
            "jenkins": "not_checked",
            "sentiment_model": "not_verified"
        }
    }), http_code


# ============================================================
# 2. NEGOCIOS CERCANOS - $near
# GET /businesses/near
# ============================================================

@app.get("/businesses/near")
def businesses_near():
    try:
        lat, lon, radius = parse_coordinates()
        limit = parse_limit()

        query = {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    },
                    "$maxDistance": radius
                }
            }
        }

        results = list(
            businesses_collection
            .find(query, {"_id": 0})
            .limit(limit)
        )

        return jsonify({
            "status": "ok",
            "method": "$near",
            "query": {
                "lat": lat,
                "lon": lon,
                "radius_meters": radius
            },
            "count": len(results),
            "results": results
        }), 200

    except ValueError as error:
        return error_response(str(error))

    except PyMongoError:
        return error_response(
            "No fue posible consultar MongoDB.", 503
        )


# ============================================================
# 3. NEGOCIOS DENTRO DE UN POLÍGONO - $geoWithin
# POST /businesses/within
# ============================================================

@app.post("/businesses/within")
def businesses_within():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return error_response("Debes enviar un objeto JSON válido.")

    try:
        polygon = validate_polygon(data.get("polygon"))

        limit = data.get("limit", 100)

        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("limit debe ser un número entero.")

        if not 1 <= limit <= MAX_LIMIT:
            raise ValueError("limit debe estar entre 1 y 100.")

        query = {
            "location": {
                "$geoWithin": {
                    "$geometry": polygon
                }
            }
        }

        results = list(
            businesses_collection
            .find(query, {"_id": 0})
            .limit(limit)
        )

        return jsonify({
            "status": "ok",
            "method": "$geoWithin",
            "count": len(results),
            "results": results
        }), 200

    except ValueError as error:
        return error_response(str(error))

    except PyMongoError:
        return error_response(
            "No fue posible consultar MongoDB.", 503
        )


# ============================================================
# 4. RESUMEN GEOGRÁFICO - $geoNear
# GET /zones/summary
# ============================================================

@app.get("/zones/summary")
def zones_summary():
    try:
        lat, lon, radius = parse_coordinates()

        pipeline = [
            {
                "$geoNear": {
                    "near": {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    },
                    "distanceField": "distance_meters",
                    "maxDistance": radius,
                    "spherical": True,
                    "key": "location"
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_businesses": {"$sum": 1},
                    "open_businesses": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$is_open", 1]},
                                1,
                                0
                            ]
                        }
                    },
                    "closed_businesses": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$is_open", 0]},
                                1,
                                0
                            ]
                        }
                    },
                    "average_stars": {"$avg": "$stars"},
                    "average_distance_meters": {
                        "$avg": "$distance_meters"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "total_businesses": 1,
                    "open_businesses": 1,
                    "closed_businesses": 1,
                    "average_stars": {"$round": ["$average_stars", 2]},
                    "average_distance_meters": {
                        "$round": ["$average_distance_meters", 2]
                    }
                }
            }
        ]

        results = list(businesses_collection.aggregate(pipeline))

        summary = results[0] if results else {
            "total_businesses": 0,
            "open_businesses": 0,
            "closed_businesses": 0,
            "average_stars": None,
            "average_distance_meters": None
        }

        total = summary["total_businesses"]

        summary["closed_percentage"] = (
            round(100 * summary["closed_businesses"] / total, 2)
            if total > 0 else None
        )

        return jsonify({
            "status": "ok",
            "method": "$geoNear",
            "query": {
                "lat": lat,
                "lon": lon,
                "radius_meters": radius
            },
            "summary": summary,
            "note": (
                "closed_percentage es la proporción de negocios "
                "con is_open=0; no es una tasa de cierre ajustada "
                "por tiempo de exposición."
            )
        }), 200

    except ValueError as error:
        return error_response(str(error))

    except PyMongoError:
        return error_response(
            "No fue posible calcular el resumen geográfico.", 503
        )


# ============================================================
# 5. ESTADÍSTICAS DE CIERRE
# GET /stats/closure
# ============================================================

@app.get("/stats/closure")
def stats_closure():
    try:
        if not db.list_collection_names().__contains__("closure_stats"):
            return jsonify({
                "status": "pending",
                "message": (
                    "Las estadísticas de cierre todavía no han sido "
                    "calculadas con Spark."
                ),
                "required": [
                    "Fechas de primera y última reseña",
                    "Días de exposición por negocio",
                    "Eventos de cierre y censura",
                    "Tasas por categoría y zona"
                ]
            }), 503

        limit = parse_limit()

        results = list(
            closure_collection
            .find({}, {"_id": 0})
            .limit(limit)
        )

        if not results:
            return jsonify({
                "status": "pending",
                "message": (
                    "La colección closure_stats existe, pero "
                    "todavía no contiene resultados."
                )
            }), 503

        return jsonify({
            "status": "ok",
            "count": len(results),
            "results": [
                serialize_document(doc)
                for doc in results
            ]
        }), 200

    except ValueError as error:
        return error_response(str(error))

    except PyMongoError:
        return error_response(
            "No fue posible consultar las estadísticas.", 503
        )


# ============================================================
# 6. PREDICCIÓN DE SENTIMIENTO
# POST /predict
# ============================================================

@app.post("/predict")
def predict():
    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return error_response("Debes enviar un objeto JSON válido.")

    text = data.get("text")

    if not isinstance(text, str) or not text.strip():
        return error_response(
            "Debes proporcionar el campo text con una reseña."
        )

    if len(text) > 10000:
        return error_response(
            "La reseña no puede superar 10000 caracteres."
        )

    return jsonify({
        "status": "pending",
        "message": (
            "El modelo Spark MLlib todavía no está entrenado "
            "ni integrado con la API."
        ),
        "model": "not_available",
        "prediction": None,
        "required": [
            "Limpieza de reseñas",
            "TF-IDF",
            "Entrenamiento Naive Bayes y Logistic Regression",
            "Evaluación con separación por business_id",
            "Guardado y carga del modelo"
        ]
    }), 503


# ============================================================
# INICIO DE FLASK
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
