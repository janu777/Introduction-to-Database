from src.BaseDataTable import BaseDataTable
import copy
import csv


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._table_name = table_name
        self._connect_info = connect_info
        self._key_columns = key_columns

        pass

    def load_csv(self):
        dir_path = self._connect_info['directory']
        file_name = self._connect_info['file_name']
        path = dir_path + file_name
        result = []
        with open(path, "r") as in_file:
            csv_file = csv.DictReader(in_file)
            for r in csv_file:
                result.append(r)
        return result

    def save_csv(self, new_csv):
        keys = new_csv[0].keys()
        dir_path = self._connect_info['directory']
        file_name = self._connect_info['file_name']
        path = dir_path + file_name
        #keys = [G_c,G_2b,G_pr,G_p,yearID,G_rf,G_ph,G_3b,playerID,GS,G_defense,G_dh,G_1b,G_cf,G_of,G_batting,lgID,G_all,G_ss,teamID,G_lf]
        with open(path, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(new_csv)

    def get_columns(self, row_result, col_list):
        result = {}
        for c in col_list:
            result[c] = row_result[c]
        return result

    def matches_template(self, row, template):
        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break
        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        result = None
        csv_data = self.load_csv()
        #print(csv_data)
        template = dict(zip(self._key_columns, key_fields))
        for row in csv_data:
            if all(item in row.items() for item in template.items()):
                result = row
                break
        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        result = None
        csv_data = self.load_csv()
        for row in csv_data:
            if all(item in row.items() for item in template.items()):
                result = row
                break
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        no_deleted_rows = 0
        new_csv = []
        csv_data = self.load_csv()
        template = dict(zip(self._key_columns, key_fields))
        for row in csv_data:
            if all(item in row.items() for item in template.items()):
                no_deleted_rows += 1
            else:
                new_csv.append(row)
        if new_csv:
            self.save_csv(new_csv)
        return no_deleted_rows

    def delete_by_template(self, template):
        """
        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        no_deleted_rows = 0
        new_csv = []
        csv_data = self.load_csv()
        for row in csv_data:
            if all(item in row.items() for item in template.items()):
                no_deleted_rows += 1
            else:
                new_csv.append(row)
        if new_csv:
            self.save_csv(new_csv)
        return no_deleted_rows

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        no_updated_rows = 0
        new_csv = []
        csv_data = self.load_csv()
        template = dict(zip(self._key_columns, key_fields))
        new_template = dict(zip(self._key_columns, new_values))
        for row in csv_data:
            if all(item in row.items() for item in template.items()):
                for k, v in template.items():
                    row[k] = new_template[k]
                no_updated_rows += 1
                new_csv.append(row)
            else:
                new_csv.append(row)
        if new_csv:
            self.save_csv(new_csv)
        return no_updated_rows

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        no_updated_rows = 0
        new_csv = []
        csv_data = self.load_csv()
        new_template = dict(zip(template.keys(), new_values))
        for row in csv_data:
            if all(item in row.items() for item in template.items()):
                for k, v in template.items():
                    row[k] = new_template[k]
                no_updated_rows += 1
                new_csv.append(row)
            else:
                new_csv.append(row)
        if new_csv:
            self.save_csv(new_csv)
        return no_updated_rows

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        new_csv_data = self.load_csv()
        new_csv_data.append(new_record)
        if new_csv_data:
            self.save_csv(new_csv_data)
        return None



    def get_rows(self):
        return self._rows
