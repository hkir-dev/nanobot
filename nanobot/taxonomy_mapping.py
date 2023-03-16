import requests
import csv
import codecs

from mapping import AbstractMapper
from contextlib import closing


class TaxonomyMapper(AbstractMapper):

    def get_source_table_name(self, display_tables):
        return get_taxonomy_name(display_tables)

    def get_source_data(self, source_table_name) :
        return get_source_data(self.db_connection, source_table_name)

    def load_target_data(self, mapping_target_config):
        target_data = dict()
        for target in mapping_target_config:
            target_data[target] = load_target_hierarchy(mapping_target_config[target])
        return target_data


def load_target_hierarchy(url):
    data = list()
    try:
        response = requests.get(url)
        content = response.iter_lines()
        headers, records = parse_nomenclature_table(content)
        for accession_id in records:
            record = records[accession_id]
            data.append({
                "entity_id": accession_id,
                "name": record["cell_set_preferred_alias"],
                "synonyms": record["cell_set_additional_aliases"],
                # TODO process parent data
                "parents": []
            })
    except requests.ConnectionError:
        # TODO exception management
        return "Connection Error"
    return data


def parse_nomenclature_table(nomenclature_content):
    headers = []
    records = dict()

    rd = csv.reader(codecs.iterdecode(nomenclature_content, 'utf-8'), delimiter=",", quotechar='"')
    row_count = 0

    id_column_name = "cell_set_accession"
    id_column = 2  # this may vary based on nomenclature, so using column_name instead
    for row in rd:
        _id = row[id_column]  # accession_id column
        if row_count == 0:
            headers = row
            if id_column_name and id_column_name in headers:
                id_column = headers.index(id_column_name)
        else:
            row_object = dict()
            for column_num, column_value in enumerate(row):
                row_object[headers[column_num]] = column_value
            records[_id] = row_object

        row_count += 1
    return headers, records


def get_source_data(db_connection, name):
    """Get content of the taxonomy from a database.

    :param CONN: db connection
    :param name: name of the taxonomy
    :return dict of: cell_set_accession -> accession details
    """
    res = db_connection.execute(
        "SELECT cell_set_accession, cell_type_name, synonyms, parent_cell_set_accession FROM " + name + " ;"
    )
    results = list()
    for db_row in res:
        synonyms = []
        parent = ""
        name = ""
        if db_row["cell_type_name"]:
            name = db_row["cell_type_name"]
        if db_row["synonyms"]:
            synonyms = str(db_row["synonyms"]).split("|")
        if db_row["parent_cell_set_accession"]:
            parent = db_row["parent_cell_set_accession"]
        results.append({
            "entity_id": db_row["cell_set_accession"],
            "name": name,
            "synonyms": synonyms,
            "parents": [parent]
        })
    return results


def get_taxonomy_name(display_tables):
    """
    Returns the name of the taxonomy being processed.
    Returns: name of the active taxonomy

    """
    cross_taxonomy_postfix = "_cross_taxonomy"
    taxonomy_name = ""
    for tbl in display_tables:
        if cross_taxonomy_postfix in str(tbl):
            taxonomy_name = str(tbl).replace(cross_taxonomy_postfix, "")
    return taxonomy_name