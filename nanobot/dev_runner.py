# Dummy runner for development purposes
import os

from nanobot import run

current_dir = os.path.dirname(os.path.realpath(__file__))

# This is a local nanobot runner used only for development and testing purposes
if __name__ == '__main__':
    print(os.getcwd())
    run(
        os.path.abspath(os.path.join(current_dir, "../dev_data/build/demo.db")),     # path to database with tables
        os.path.abspath(os.path.join(current_dir, "../dev_data/curation_tables/table.tsv")),    # path to "table" table
        base_ontology="demo",  # name of base ontology for project
        default_params={"view": "tree"},
        default_table="table",
        hide_index=True,
        import_table="import",
        max_children=100,
        title="CCN2 Taxonomy Editor",          # project title to display in header
        flask_host="127.0.0.1",
        flask_port="5554",
    )
    print("out")
