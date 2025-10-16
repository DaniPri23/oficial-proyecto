import logging
import pandas as pd
from neo4j import GraphDatabase
import io

class Database:
    def __init__(self, uri, user, password, db_name):
        """
        Inicializa la conexión con la base de datos Neo4j.
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.db_name = db_name
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            self.driver.verify_connectivity()
        except Exception as e:
            logging.error(f"Error al conectar con Neo4j: {e}")
            raise

    def close(self):
        if self.driver is not None:
            self.driver.close()

    def _get_session(self):
        """
        Método de ayuda para obtener una sesión para la base de datos correcta.
        """
        return self.driver.session(database=self.db_name)

    # --- Métodos de Ayuda Internos ---
    def _execute_query(self, query, parameters=None):
        """Función de ayuda para ejecutar consultas de LECTURA."""
        with self._get_session() as session:
            result = session.execute_read(lambda tx: tx.run(query, parameters).data())
            return result

    def _execute_write(self, tx, query, parameters=None):
        """Función de ayuda para ser usada DENTRO de una transacción de escritura."""
        tx.run(query, parameters)

    def get_all_nodes(self, entity_label):
        """
        Obtiene todos los nodos de una etiqueta específica de forma explícita y segura.
        """
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels:
            logging.warning(f"Intento de acceso a una etiqueta no permitida: {entity_label}")
            return []

        capitalized_label = entity_label.capitalize()
        
        entity_details = {
            'Persona': {'props': ['nombreCompleto', 'tipoLector'], 'id': 'nombreCompleto'},
            'Libro': {'props': ['titulo', 'generoLiterario', 'añoPublicacion'], 'id': 'titulo'},
            'Autor': {'props': ['nombreCompleto', 'nacionalidad'], 'id': 'nombreCompleto'},
            'Club': {'props': ['nombre', 'ubicacion', 'tematica'], 'id': 'nombre'}
        }

        details = entity_details[capitalized_label]
        props_to_return = ', '.join([f'n.{prop} AS {prop}' for prop in details['props']])
        identifier = details['id']

        query = f"MATCH (n:{capitalized_label}) RETURN {props_to_return} ORDER BY n.{identifier}"
        
        return self._execute_query(query)

    def add_node(self, entity_label, properties):
        """
        Agrega un nuevo nodo a la base de datos con la sintaxis Cypher correcta.
        """
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels:
            raise ValueError("Etiqueta de entidad no válida.")
        
        capitalized_label = entity_label.capitalize()
        
        set_clauses = ', '.join([f"n.{key} = ${key}" for key in properties.keys()])
        query = f"CREATE (n:{capitalized_label}) SET {set_clauses}"
        
        with self._get_session() as session:
            session.execute_write(self._execute_write, query, properties)
    
    def get_identifier_property(self, entity_label):
        """
        Función de ayuda para obtener la propiedad que funciona como identificador único.
        """
        capitalized_label = entity_label.capitalize()
        if capitalized_label == "Libro":
            return "titulo"
        elif capitalized_label == "Club":
            return "nombre"
        return "nombreCompleto"

    def update_node(self, entity_label, identifier, properties):
        """
        Actualiza las propiedades de un nodo existente.
        """
        allowed_labels = ['persona', 'libro', 'autor', 'club']
        if entity_label.lower() not in allowed_labels:
            raise ValueError("Etiqueta de entidad no válida.")

        capitalized_label = entity_label.capitalize()
        id_property = self.get_identifier_property(entity_label)

        properties.pop(id_property, None)

        set_clauses = ', '.join([f"n.{key} = ${key}" for key in properties.keys()])

        if not set_clauses:
            return

        query = f"""
        MATCH (n:{capitalized_label} {{{id_property}: $identifier}})
        SET {set_clauses}
        """
        
        parameters = {"identifier": identifier, **properties}

        with self._get_session() as session:
            session.execute_write(self._execute_write, query, parameters)
    
    def crear_relaciones(self, tipo_relacion, from_node_id, to_node_ids):
        relaciones_map = {
            'autoria': {'from_label': 'Autor', 'to_label': 'Libro', 'rel_type': 'ESCRIBIO', 'from_prop': 'nombreCompleto', 'to_prop': 'titulo'},
            'membresia': {'from_label': 'Persona', 'to_label': 'Club', 'rel_type': 'PERTENECE_A', 'from_prop': 'nombreCompleto', 'to_prop': 'nombre'},
            'lectura': {'from_label': 'Persona', 'to_label': 'Libro', 'rel_type': 'LEE', 'from_prop': 'nombreCompleto', 'to_prop': 'titulo'},
            'recomendacion': {'from_label': 'Club', 'to_label': 'Libro', 'rel_type': 'RECOMIENDA', 'from_prop': 'nombre', 'to_prop': 'titulo'}
        }

        if tipo_relacion not in relaciones_map:
            raise ValueError("Tipo de relación no válido.")

        config = relaciones_map[tipo_relacion]
        
        query = f"""
        MATCH (a:{config['from_label']} {{{config['from_prop']}: $from_id}})
        UNWIND $to_ids AS to_id
        MATCH (b:{config['to_label']} {{{config['to_prop']}: to_id}})
        MERGE (a)-[:{config['rel_type']}]->(b)
        """
        parameters = {"from_id": from_node_id, "to_ids": to_node_ids}
        
        with self._get_session() as session:
            session.execute_write(self._execute_write, query, parameters)

    def cargar_datos_iniciales(self):
        with self._get_session() as session:
            session.execute_write(self._execute_write, "MATCH (n) DETACH DELETE n")
            logging.info("Base de datos anterior eliminada.")

            queries_indices = [
                "CREATE CONSTRAINT persona_nombre IF NOT EXISTS FOR (p:Persona) REQUIRE p.nombreCompleto IS UNIQUE",
                "CREATE CONSTRAINT libro_titulo IF NOT EXISTS FOR (l:Libro) REQUIRE l.titulo IS UNIQUE",
                "CREATE CONSTRAINT autor_nombre IF NOT EXISTS FOR (a:Autor) REQUIRE a.nombreCompleto IS UNIQUE",
                "CREATE INDEX persona_csv_id IF NOT EXISTS FOR (p:Persona) ON (p.csvId)",
                "CREATE INDEX libro_csv_id IF NOT EXISTS FOR (l:Libro) ON (l.csvId)",
                "CREATE INDEX autor_csv_id IF NOT EXISTS FOR (a:Autor) ON (a.csvId)",
                "CREATE INDEX club_csv_id IF NOT EXISTS FOR (c:Club) ON (c.csvId)"
            ]
            for query in queries_indices:
                session.execute_write(self._execute_write, query)
            logging.info("Restricciones e índices creados/verificados.")

            queries_nodos = [
                "LOAD CSV WITH HEADERS FROM 'file:///Persona.csv' AS row FIELDTERMINATOR ';' CREATE (p:Persona {nombreCompleto: row.Nombre, tipoLector: row.TipoLector, csvId: toInteger(row.id)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Autor.csv' AS row FIELDTERMINATOR ';' CREATE (a:Autor {nombreCompleto: row.Nombre, nacionalidad: row.Nacionalidad, csvId: toInteger(row.idAutor)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Libro.csv' AS row FIELDTERMINATOR ';' CREATE (l:Libro {titulo: row.Titulo, generoLiterario: row.Genero, añoPublicacion: toInteger(row.Anno), csvId: toInteger(row.IdLibro)})",
                "LOAD CSV WITH HEADERS FROM 'file:///Club.csv' AS row FIELDTERMINATOR ';' CREATE (c:Club {nombre: row.Nombre, ubicacion: row.Ubicacion, tematica: row.Tematica, csvId: toInteger(row.IdClub)})"
            ]
            for query in queries_nodos:
                session.execute_write(self._execute_write, query)
            logging.info("Nodos cargados desde archivos CSV.")
            
            queries_relaciones = [
                "LOAD CSV WITH HEADERS FROM 'file:///Autor-libro.csv' AS row FIELDTERMINATOR ';' MATCH (a:Autor {csvId: toInteger(row.idAutor)}) MATCH (l:Libro {csvId: toInteger(row.idLibro)}) MERGE (a)-[:ESCRIBIO]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Persona-libro.csv' AS row FIELDTERMINATOR ';' MATCH (p:Persona {csvId: toInteger(row.id)}) MATCH (l:Libro {csvId: toInteger(row.idLibro)}) MERGE (p)-[:LEE]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Club-libro.csv' AS row FIELDTERMINATOR ';' MATCH (c:Club {csvId: toInteger(row.idClub)}) MATCH (l:Libro {csvId: toInteger(row.idLibro)}) MERGE (c)-[:RECOMIENDA]->(l)",
                "LOAD CSV WITH HEADERS FROM 'file:///Persona-club2.csv' AS row FIELDTERMINATOR ';' MATCH (p:Persona {csvId: toInteger(row.idPersona)}) MATCH (c:Club {csvId: toInteger(row.idClub)}) MERGE (p)-[:PERTENECE_A]->(c)"
            ]
            for query in queries_relaciones:
                session.execute_write(self._execute_write, query)
            logging.info("Relaciones creadas exitosamente.")

        return "Todos los datos han sido cargados exitosamente en Neo4j."

    def consulta_libros_leidos(self, persona_nombre):
        query = "MATCH (p:Persona {nombreCompleto: $nombre})-[r:LEE]->(l:Libro) RETURN l.titulo AS titulo, l.generoLiterario AS genero"
        records = self._execute_query(query, {"nombre": persona_nombre})
        return [{"titulo": record['titulo'], "genero": record['genero']} for record in records]

    def consulta_personas_club(self, club_nombre):
        query = "MATCH (p:Persona)-[:PERTENECE_A]->(c:Club {nombre: $nombre}) RETURN p.nombreCompleto AS nombre"
        return self._execute_query(query, {"nombre": club_nombre})

    def consulta_personas_mas_libros(self):
        query = """
        MATCH (p:Persona)-[:LEE]->(l:Libro)<-[:RECOMIENDA]-(c:Club)
        WITH p, c, count(l) AS librosRecomendadosLeidos
        WHERE librosRecomendadosLeidos >= 3
        RETURN p.nombreCompleto AS persona, c.nombre AS club
        """
        return self._execute_query(query)

    def consulta_personas_mas_clubes(self):
        query = """
        MATCH (p:Persona)-[:PERTENECE_A]->(c:Club)
        WITH p, count(c) AS numeroClubes
        WHERE numeroClubes > 1
        MATCH (p)-[:PERTENECE_A]->(club:Club)
        RETURN p.nombreCompleto AS persona, collect(club.nombre) AS clubes
        """
        return self._execute_query(query)

    def consulta_libros_populares(self):
        query = """
        MATCH (p:Persona)-[:LEE]->(l:Libro)
        RETURN l.titulo AS titulo, count(p) AS lectores
        ORDER BY lectores DESC
        LIMIT 3
        """
        return self._execute_query(query)
    
    def clean_value(self, val):
        """Limpia valores de dataframe: convierte a string y hace strip, ignora NaN."""
        if pd.isna(val):
            return None
        return str(val).strip()

    def read_csv_flexible(self, file_storage):
        """Lee un CSV con diferentes separadores posibles y normaliza columnas."""
        contents = file_storage.read()
        file_storage.seek(0)
        for sep in [';', ',', '\t', '|']:
            try:
                df = pd.read_csv(io.BytesIO(contents), sep=sep, engine='python')
                if df.shape[1] > 1:
                    df.columns = df.columns.str.strip().str.lower()
                    return df
            except Exception:
                continue
        return None


    # ======================================
    # CARGA DE NODOS
    # ======================================
    def cargar_autores(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MERGE (a:Autor {id: toInteger($id)})
                SET a.nombre = $nombre,
                    a.nacionalidad = $nacionalidad
                """
                params = {
                    "id": self.clean_value(row.get("idautor")),
                    "nombre": self.clean_value(row.get("nombre")),
                    "nacionalidad": self.clean_value(row.get("nacionalidad"))
                }
                session.execute_write(self._execute_write, query, params)

    def cargar_personas(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MERGE (p:Persona {id: toInteger($id)})
                SET p.nombre = $nombre,
                    p.tiplector = $tipo
                """
                params = {
                    "id": self.clean_value(row.get("id")),
                    "nombre": self.clean_value(row.get("nombre")),
                    "tipo": self.clean_value(row.get("tipolector"))
                }
                session.execute_write(self._execute_write, query, params)

    def cargar_libros(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MERGE (l:Libro {id: toInteger($id)})
                SET l.titulo = $titulo,
                    l.genero = $genero,
                    l.anno = toInteger($anno)
                """
                params = {
                    "id": self.clean_value(row.get("idlibro")),
                    "titulo": self.clean_value(row.get("titulo")),
                    "genero": self.clean_value(row.get("genero")),
                    "anno": self.clean_value(row.get("anno"))
                }
                session.execute_write(self._execute_write, query, params)

    def cargar_clubes(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MERGE (c:Club {id: toInteger($id)})
                SET c.nombre = $nombre,
                    c.ubicacion = $ubicacion,
                    c.tematica = $tematica
                """
                params = {
                    "id": self.clean_value(row.get("idclub")),
                    "nombre": self.clean_value(row.get("nombre")),
                    "ubicacion": self.clean_value(row.get("ubicacion")),
                    "tematica": self.clean_value(row.get("tematica"))
                }
                session.execute_write(self._execute_write, query, params)

    # ======================================
    # CARGA DE RELACIONES
    # ======================================
    def cargar_relacion_autor_libro(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MATCH (a:Autor {id: toInteger($autor_id)})
                MATCH (l:Libro {id: toInteger($libro_id)})
                MERGE (a)-[:ESCRIBIO]->(l)
                """
                params = {
                    "autor_id": self.clean_value(row.get("idautor")),
                    "libro_id": self.clean_value(row.get("idlibro"))
                }
                session.execute_write(self._execute_write, query, params)

    def cargar_relacion_persona_libro(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MATCH (p:Persona {id: toInteger($persona_id)})
                MATCH (l:Libro {id: toInteger($libro_id)})
                MERGE (p)-[:LEE]->(l)
                """
                params = {
                    "persona_id": self.clean_value(row.get("id") or row.get("idpersona")),
                    "libro_id": self.clean_value(row.get("idlibro"))
                }
                session.execute_write(self._execute_write, query, params)

    def cargar_relacion_persona_club(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MATCH (p:Persona {id: toInteger($persona_id)})
                MATCH (c:Club {id: toInteger($club_id)})
                MERGE (p)-[:PERTENECE_A]->(c)
                """
                params = {
                    "persona_id": self.clean_value(row.get("idpersona")),
                    "club_id": self.clean_value(row.get("idclub"))
                }
                session.execute_write(self._execute_write, query, params)

    def cargar_relacion_club_libro(self, df):
        with self._get_session() as session:
            for _, row in df.iterrows():
                query = """
                MATCH (c:Club {id: toInteger($club_id)})
                MATCH (l:Libro {id: toInteger($libro_id)})
                MERGE (c)-[:RECOMIENDA]->(l)
                """
                params = {
                    "club_id": self.clean_value(row.get("idclub")),
                    "libro_id": self.clean_value(row.get("idlibro"))
                }
                session.execute_write(self._execute_write, query, params)
