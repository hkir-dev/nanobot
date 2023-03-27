from abc import ABC, abstractmethod


class AbstractMapper(ABC):
    """
    Abstract base class for mapping capabilities
    """

    def set_db_connection(self, db_connection):
        self.db_connection = db_connection

    @abstractmethod
    def is_mapping_table(self, table_name):
        """
        Identifies if the given table is a mapping table.
        Args:
            table_name: name of the table to check

        Returns: True if table is a mapping table, False otherwise
        """
        pass

    @abstractmethod
    def get_source_table_name(self, display_tables) -> str:
        """
        Returns the name of the table/hierarchy to be mapped.
        Args:
            display_tables: name of the existing tables

        Returns: name of the table/hierarchy to be mapped.

        """
        pass

    @abstractmethod
    def get_source_data(self, source_table_name):
        """
        Returns the mappings source table data
        Args:
            source_table_name: name of the source table

        Returns: source data in flat format
        """
        pass

    @abstractmethod
    def get_source_tree(self, source_table_name):
        """
        Returns the mappings source table data in hierarchical format. Uses parent_cell_set_accession field to build a
        nested source data representation.
        Args:
            source_table_name: name of the source table

        Returns: source data in nested tree format.
        """
        pass

    @abstractmethod
    def load_target_data(self, mapping_target_config):
        """
        Loads mapping target's data using the given configurations in both flat and tree representation.
        Args:
            mapping_target_config: mapping target configurations
        Returns: target_data_flat, target_data_tree
        """
        pass
