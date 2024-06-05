"""
This module defines an abstract base class for storage mechanisms.
It provides a standardized interface for saving data, allowing for implementation
of various storage backends such as file systems, databases, or cloud storage solutions.
"""

from abc import ABC, abstractmethod


# pylint: disable=R0903
class BaseStorage(ABC):
    """
    An abstract base class that represents a generic storage mechanism.

    This class is designed to be subclassed by specific storage implementations,
    providing a consistent interface for saving data across various storage backends.
    """

    @abstractmethod
    def save_data(self, data, config):
        """
        Abstract method to save data using the provided configuration.

        This method must be implemented by subclasses to handle the actual
        data storage logic according to the specific backend's requirements.

        Parameters:
            data: The data to be saved. 
            config: Configuration settings for the storage operation. 
        """
