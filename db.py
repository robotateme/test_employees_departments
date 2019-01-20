import sqlite3
import json


class Database:
    """
        Класс описывающий соединение с базой данных
    """
    table_name = 'employees_departments'
    _db_connection = None
    _db_cursor = None

    def __init__(self, db_path='storage/database/test_task.db'):
        self._db_connection = sqlite3.connect(db_path)
        self._db_cursor = self._db_connection.cursor()

    def query(self, query, params):
        """
            Метод реализующий выполнение запросов принимает стандарные параметры для метода sqlite3.execute()
        """
        return self._db_cursor.execute(query, params)

    def __del__(self):
        self._db_connection.close()


class DBManager(Database):
    office = 1
    department = 2
    employee = 3

    def get_colleagues_by_user_id(self, user_id):
        """
            Производит рекурсивную выборку соседних пработников из офиса по ID-пользователя
        :param user_id:
        :return:
        """
        self._db_cursor.row_factory = sqlite3.Row
        sql = """WITH RECURSIVE tree AS (SELECT id AS id,
                               0  AS number_of_ancestors,
                               id  AS ancestry,
                               ParentId,
                               Name,
                               Type,
                               id AS start_of_ancestry

                        FROM employees_departments
                        WHERE ParentId IS NULL

    UNION

    SELECT child.id                                    AS id,
           tree.number_of_ancestors + 1                AS ancestry_size,
           tree.ancestry || ',' || child.id            AS ancestry,
           child.ParentId,
           child.Name,
           child.Type,
           COALESCE(tree.start_of_ancestry, child.ParentId) AS start_of_ancestry
    FROM employees_departments AS child
           INNER JOIN tree ON tree.id = child.ParentId)
    SELECT tree2.*
    FROM tree
       JOIN tree as tree2 ON tree.Type=tree2.Type AND tree.start_of_ancestry=tree2.start_of_ancestry
    WHERE tree.id = ?"""
        return self.query(sql, (user_id,))


class DBSchemaManager(Database):
    """
        Класс менеджер для работы со схемой, исключительно.
        Загрузки схемы и данных
    """
    def restore(self, schema_path='storage/database/schema.sql'):
        """
        Производит загрузку схемы из дампа, принимает путь до файла схемы, для возможного улучшения работы приложения
        :param schema_path:
        :return:
        """
        with open(schema_path) as fp:
            self._db_cursor.executescript(fp.read())

    def save_json_data(self, file_path='storage/files/example.json'):
        """
        Загружает данные из JSON-файла, возможно улучшение с использовнаием генераторов, для загрузки больших данных
        :param file_path:
        :return:
        """
        with open(file_path) as fp:
            data = json.load(fp)
            for row in data:
                row.pop('id', None)
                columns = ', '.join(row.keys())
                placeholders = ','.join('?' * len(row))
                sql = 'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})' \
                    .format(table_name=self.table_name,
                            columns=columns,
                            placeholders=placeholders)
                self.query(sql, (row['ParentId'], row['Name'], row['Type']))
                self._db_connection.commit()
