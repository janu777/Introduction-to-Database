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
        result = []
        result1 = []
        csv_data = self.load_csv()
        if key_fields is not None:
            for row in csv_data:
                for i in range(len(key_fields)):
                    if key_fields[i] in row.values():
                        if i == len(key_fields) - 1:
                            result1.append(row)
                            break
                    else:
                        break
            if field_list is not None:
                for i in result1:
                    result.append(self.get_columns(i, field_list))
            else:
                result = result1
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
        result = []
        result1 = []
        csv_data = self.load_csv()
        if template is not None:
            for row in csv_data:
                if self.matches_template(row, template):
                    result1.append(row)
            if field_list is not None:
                for i in result1:
                    result.append(self.get_columns(i, field_list))
            else:
                result = result1
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        result = []
        no_deleted_rows = 0
        csv_data = self.load_csv()
        if key_fields is not None:
            for row in csv_data:
                for i in range(len(key_fields)):
                    if key_fields[i] in row.values():
                        if i == len(key_fields) - 1:
                            no_deleted_rows += 1
                            break
                    else:
                        result.append(row)
                        break
        return no_deleted_rows

    def delete_by_template(self, template):
        """
        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        result = []
        no_deleted_rows = 0
        csv_data = self.load_csv()
        if template is not None:
            for row in csv_data:
                if self.matches_template(row, template):
                    no_deleted_rows +=1
                else:
                    result.append(row)
        return no_deleted_rows

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        result = []
        no_updated_rows = 0
        csv_data = self.load_csv()
        if key_fields is not None:
            for row in csv_data:
                update_row = row
                for i in range(len(key_fields)):
                    if key_fields[i] in row.values():
                        listOfKeys = [key for (key, value) in row.items() if value == key_fields[i]]
                        update_row = update_row.fromkeys(listOfKeys, new_values[i])
                        if i == len(key_fields) - 1:
                            result.append(update_row)
                            no_updated_rows += 1
                            break
                    else:
                        result.append(row)
                        break
        return no_updated_rows

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        result = []
        no_updated_rows = 0
        csv_data = self.load_csv()
        if template is not None:
            for row in csv_data:
                update_row = row
                if self.matches_template(row, template):
                    for k, v in template.items():
                        update_row[k]=new_values[k]
                    result.append(update_row)
                    no_updated_rows += 1
                else:
                    result.append(row)
        return no_updated_rows

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        csv_data = self.load_csv()
        csv_data.append(list(new_record))
        return None

    def get_rows(self):
        return self._rows
