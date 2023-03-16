from mapping import AbstractMapper

class TaxonomyMapper(AbstractMapper):

    def get_source_table_name(self, display_tables):
        return get_taxonomy_name(display_tables)

    def get_source_data(self, source_table_name) :
        return get_source_data(self.db_connection, source_table_name)



def load_target_hierarchy(name, url):
    try:
        response = requests.get(url)
        text = response.iter_lines()
    except requests.ConnectionError:
        return "Connection Error"


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