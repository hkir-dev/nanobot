from abc import ABC, abstractmethod

class AbstractMapper(ABC):
    """
    Abstract base class for mapping capabilities
    """

    def set_db_connection(self, db_connection):
        self.db_connection = db_connection

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
    def get_source_data(self, source_table_name) :
        """
        Retourns the mappings source table data
        Args:
            source_table_name: name of the source table

        Returns:

        """
        pass