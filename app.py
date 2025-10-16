from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from db import Database
import logging
from werkzeug.utils import secure_filename
import os

app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Dani20052309*"  
NEO4J_DATABASE = "proyecto"               

try:
    db = Database(
        uri=NEO4J_URI, 
        user=NEO4J_USER, 
        password=NEO4J_PASSWORD,
        db_name=NEO4J_DATABASE
    )
    logging.info(f"Conexión con Neo4j (base de datos '{NEO4J_DATABASE}') establecida exitosamente.")
except Exception as e:
    logging.error(f"No se pudo conectar a Neo4j: {e}")
    db = None

@app.route('/')
def serve_index():
    return send_from_directory(app.static_folder, 'interfaz.html')


ENTITY_MAP = {
    "personas": "persona",
    "libros": "libro",
    "autores": "autor",
    "clubes": "club"
}

@app.route('/<plural_entity>', methods=['GET', 'POST'])
def handle_entities(plural_entity):
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500

    entity = ENTITY_MAP.get(plural_entity.lower())
    if not entity:
        return jsonify({"error": f"La entidad '{plural_entity}' no es válida."}), 404

    if request.method == 'GET':
        try:
            nodes = db.get_all_nodes(entity)
            return jsonify(nodes)
        except Exception as e:
            logging.error(f"Error al obtener entidades '{entity}': {e}")
            return jsonify({"error": f"Error interno al obtener {entity}s"}), 500

    if request.method == 'POST':
        data = request.json
        if not data:
            return jsonify({"error": "No se proporcionaron datos."}), 400
        try:
            db.add_node(entity, data)
            return jsonify({"message": f"{entity.capitalize()} agregado correctamente."}), 201
        except Exception as e:
            logging.error(f"Error al agregar entidad '{entity}': {e}")
            return jsonify({"error": f"Error interno al agregar {entity}"}), 500

@app.route('/<plural_entity>/<identifier>', methods=['PUT'])
def update_entity(plural_entity, identifier):
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500

    entity = ENTITY_MAP.get(plural_entity.lower())
    if not entity:
        return jsonify({"error": f"La entidad '{plural_entity}' no es válida."}), 404

    data = request.json
    if not data:
        return jsonify({"error": "No se proporcionaron datos para actualizar."}), 400

    try:
        db.update_node(entity, identifier, data)
        return jsonify({"message": f"{entity.capitalize()} '{identifier}' actualizado correctamente."}), 200
    except Exception as e:
        logging.error(f"Error al actualizar entidad '{entity}': {e}")
        return jsonify({"error": f"Error interno al actualizar {entity}"}), 500

@app.route('/relaciones/<tipo_relacion>', methods=['POST'])
def crear_relacion(tipo_relacion):
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500
        
    data = request.json
    from_node = data.get('from')
    to_nodes = data.get('to')

    if not from_node or not to_nodes:
        return jsonify({"error": "Datos incompletos para crear la relación."}), 400
    
    try:
        db.crear_relaciones(tipo_relacion, from_node, to_nodes)
        return jsonify({"message": "Relaciones creadas exitosamente."}), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.error(f"Error al crear relación '{tipo_relacion}': {e}")
        return jsonify({"error": "Error interno al crear las relaciones."}), 500

@app.route('/consultas/libros-leidos', methods=['GET'])
def get_libros_leidos():
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500
    persona_nombre = request.args.get('persona')
    if not persona_nombre:
        return jsonify({"error": "El nombre de la persona es requerido."}), 400
    try:
        resultado = db.consulta_libros_leidos(persona_nombre)
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'libros leidos': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/personas-club', methods=['GET'])
def get_personas_club():
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500
    club_nombre = request.args.get('club')
    if not club_nombre:
        return jsonify({"error": "El nombre del club es requerido."}), 400
    try:
        resultado = db.consulta_personas_club(club_nombre)
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'personas club': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/personas-mas-libros', methods=['GET'])
def get_personas_mas_libros():
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        resultado = db.consulta_personas_mas_libros()
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'personas mas libros': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/personas-mas-clubes', methods=['GET'])
def get_personas_mas_clubes():
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        resultado = db.consulta_personas_mas_clubes()
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'personas mas clubes': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

@app.route('/consultas/libros-populares', methods=['GET'])
def get_libros_populares():
    if not db:
        return jsonify({"error": "La base de datos no está disponible."}), 500
    try:
        resultado = db.consulta_libros_populares()
        return jsonify(resultado)
    except Exception as e:
        logging.error(f"Error en consulta 'libros populares': {e}")
        return jsonify({"error": "Error al procesar la consulta."}), 500

from werkzeug.utils import secure_filename
import os

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/admin/subir-csv', methods=['POST'])
def subir_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No se envió ningún archivo"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "El nombre del archivo está vacío"}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Solo se permiten archivos CSV"}), 400

    filename = secure_filename(file.filename.lower())
    try:
        df = db.read_csv_flexible(file)
        if df is None or df.empty:
            return jsonify({"error": "El archivo CSV está vacío o no se pudo leer"}), 400

        if filename == "autor.csv":
            db.cargar_autores(df)
        elif filename == "libro.csv":
            db.cargar_libros(df)
        elif filename == "persona.csv":
            db.cargar_personas(df)
        elif filename == "club.csv":
            db.cargar_clubes(df)
        elif filename == "autor-libro.csv":
            db.cargar_relacion_autor_libro(df)
        elif filename == "persona-libro.csv":
            db.cargar_relacion_persona_libro(df)
        elif filename == "persona-club.csv":
            db.cargar_relacion_persona_club(df)
        elif filename == "club-libro.csv":
            db.cargar_relacion_club_libro(df)
        else:
            return jsonify({"error": f"Nombre de archivo no reconocido: {filename}"}), 400

        return jsonify({"message": f"{filename} cargado correctamente"}), 200

    except Exception as e:
        logging.error(f"❌ Error al procesar {filename}: {e}")
        return jsonify({"error": f"Error al cargar {filename}: {e}"}), 500

if __name__ == '__main__':
    app.run(debug=True)
