class ETL:
    def __init__(self, conn, loader):
        self.conn = conn
        self.loader = loader

    def extract_addresses(self):
        with self.conn.cursor() as cursor:
            sql = '''SELECT avtomat_number, house, street
                     FROM avtomat AS a
                     INNER JOIN
                     street AS s
                     ON a.street_id = s.id LIMIT 10'''
            cursor.execute(sql)
            res = cursor.fetchall()
        return res

    def _transform_row(self, row):
        return {
            'id': row['avtomat_number'],
            'address': f"{row['street']} {row['house']}"
        }

    def load(self, index_name):
        records = [self._transform_row(row)
                   for row in self.extract_addresses()]
        self.loader.load_to_es(records, index_name)
