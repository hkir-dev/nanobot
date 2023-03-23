import requests
import csv
import codecs

from mapping import AbstractMapper
import networkx as nx


class TaxonomyMapper(AbstractMapper):

    def get_source_table_name(self, display_tables):
        return get_taxonomy_name(display_tables)

    def get_source_data(self, source_table_name) :
        return get_source_data(self.db_connection, source_table_name)

    def get_source_tree(self, source_table_name):
        return get_source_tree(self.get_source_data(source_table_name))

    def load_target_data(self, mapping_target_config):
        target_data_flat = dict()
        target_data_tree = dict()
        for target in mapping_target_config:
            target_data = load_target_hierarchy(mapping_target_config[target])
            target_data_flat[target] = target_data[0]
            target_data_tree[target] = target_data[1]
        return target_data_flat, target_data_tree


def load_target_hierarchy(url):
    flat_data = list()
    tree_data = list()
    try:
        response = requests.get(url)
        content = response.iter_lines()
        headers, records = parse_nomenclature_table(content)
        dend = nomenclature_2_nodes_n_edges(records)
        flat_data = dend['nodes'].values()
        tree_data = construct_tree_hierarchy(dend['nodes'], dend['edges'])
        # for accession_id in records:
        #     record = records[accession_id]
        #     data.append({
        #         "entity_id": accession_id,
        #         "name": record["cell_set_preferred_alias"],
        #         "synonyms": record["cell_set_additional_aliases"],
        #         # TODO process parent data
        #         "parents": []
        #     })
    except requests.ConnectionError:
        # TODO exception management
        return "Connection Error"
    return flat_data, tree_data


def construct_tree_hierarchy(nodes, edges):
    """
    Processes dendrogram nodes and edges to generate a nested object hierarchy
    Args:
        nodes: dendrogram nodes
        edges: dendrogram edges
    Returns: nested nodes hierarchy
    """
    tree_data = list()
    tree = nx.DiGraph()
    for edge in edges:
        tree.add_edge(edge[1], edge[0])

    root_nodes = [x for x in tree.nodes(data=True) if tree.in_degree(x[0]) == 0]
    for node in root_nodes:
        root = populate_node_data(tree, nodes, node[0])
        root["expanded"] = True
        tree_data.append(root)
    return tree_data


def populate_node_data(tree, nodes, accession_id):
    node = dict()

    children = list()
    descendants = tree.successors(accession_id)
    for descendant in descendants:
        children.append(populate_node_data(tree, nodes, descendant))

    node["id"] = accession_id
    text = accession_id
    if nodes[accession_id]["name"]:
        text = nodes[accession_id]["name"] + "  [" + accession_id + "]"
    node["text"] = text
    if len(children) == 1:
        node["expanded"] = True
    if children:
        node["nodes"] = children
    return node


def parse_nomenclature_table(nomenclature_content):
    headers = []
    records = dict()

    rd = csv.reader(codecs.iterdecode(nomenclature_content, 'utf-8'), delimiter=",", quotechar='"')
    row_count = 0

    id_column_name = "cell_set_accession"
    id_column = 2  # this may vary based on nomenclature, so automatically updated based on id_column_name
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
            "parent": parent
        })
    return results


def get_source_tree(flat_data):
    nodes = dict()
    edges = set()
    for data in flat_data:
        nodes[data["entity_id"]] = data
        if data["parent"]:
            edges.add((data["entity_id"], data["parent"]))

    return construct_tree_hierarchy(nodes, edges)


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


def nomenclature_2_nodes_n_edges(nomenclature_records):
    out = dict()
    out['nodes'] = dict()
    out['edges'] = set()

    child_cell_sets = list()
    for node_cell_set_accession in nomenclature_records:
        record_dict = nomenclature_records[node_cell_set_accession]
        # node = {prop: record_dict[prop] for prop in nomenclature_headers}
        node = {
                "entity_id": node_cell_set_accession,
                "name": record_dict["cell_set_preferred_alias"],
                "synonyms": record_dict["cell_set_additional_aliases"],
                "parents": []
        }
        out['nodes'][node_cell_set_accession] = node

        children_str = record_dict['child_cell_set_accessions']
        if children_str:
            children = set(children_str.strip().split('|'))
        else:
            children = {node_cell_set_accession}
        node = {"node_cell_set_accession": node_cell_set_accession, "children": children}
        child_cell_sets.append(node)

    sorted_child_cell_sets = sorted(child_cell_sets, key=lambda x: len(x["children"]))
    for child_cell_sets in sorted_child_cell_sets:
        parent_node = find_next_inclusive_node(sorted_child_cell_sets, child_cell_sets)
        if parent_node:
            out['edges'].add((child_cell_sets["node_cell_set_accession"], parent_node["node_cell_set_accession"]))

    fix_multi_inheritance_relations(out, sorted_child_cell_sets)
    return out


def find_next_inclusive_node(sorted_child_cell_sets, current_node):
    """
    Find the first node whose children are the minimal container of the children of the current node.
    Args:
        sorted_child_cell_sets: list of the nodes and their children
        current_node: node to search its parent node.

    Returns: parent node info
    """
    is_consecutive = False
    for child_cell_sets in sorted_child_cell_sets:
        if is_consecutive and current_node["children"].issubset(child_cell_sets["children"]):
            return child_cell_sets
        if child_cell_sets == current_node:
            is_consecutive = True

    return None


def fix_multi_inheritance_relations(out, sorted_child_cell_sets):
    """
    Different from json dendrograms, (mouse) nomenclature tsv supports multi-inheritance. This function identifies
    multi-inheritance cases and accordingly generates new edges.

    There is multi-inheritance if a node is leaf but also has a children definition in the nomenclature.

    Args:
        out: single inheritance taxonomy
        sorted_child_cell_sets: list of the nodes and their children
    Returns: updated taxonomy
    """
    leaf_nodes = find_leaf_nodes(out['edges'])
    multi_inheritance_nodes = get_multi_inheritance_nodes(out, leaf_nodes, sorted_child_cell_sets)

    for mi_node in multi_inheritance_nodes:
        is_consecutive = False
        children = mi_node["children"].copy()
        for child_cell_sets in reversed(sorted_child_cell_sets):
            if is_consecutive and child_cell_sets["children"].issubset(children):
                out['edges'].add((child_cell_sets["node_cell_set_accession"], mi_node["node_cell_set_accession"]))
                children = children - child_cell_sets["children"]
            if child_cell_sets == mi_node:
                is_consecutive = True


def get_multi_inheritance_nodes(out, leaf_nodes, sorted_child_cell_sets):
    multi_inheritance_nodes = list()
    dend_tree = generate_dendrogram_tree(out)

    for node in sorted_child_cell_sets:
        descendants = nx.descendants(dend_tree, node["node_cell_set_accession"])
        for child in node["children"]:
            if child not in descendants and child != node["node_cell_set_accession"] \
                    and node not in multi_inheritance_nodes:
                multi_inheritance_nodes.append(node)

    return multi_inheritance_nodes


def find_leaf_nodes(edges):
    leaf_nodes = set()
    for edge in edges:
        leaf_nodes.add(edge[0])

    for edge in edges:
        if edge[1] in leaf_nodes:
            leaf_nodes.remove(edge[1])

    return leaf_nodes


def generate_dendrogram_tree(dendrogram_data):
    """
    Generates a tree representation using the edges of the dendrogram data.
    Args:
        dendrogram_data: Parsed dendrogram file data

    Returns: networkx directed graph that represents the taxonomy

    """
    tree = nx.DiGraph()
    for edge in dendrogram_data['edges']:
        tree.add_edge(edge[1], edge[0])

    return tree
