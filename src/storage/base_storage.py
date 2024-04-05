from abc import ABC, abstractmethod

class BaseStorage(ABC):
    @abstractmethod
    def save_data(self, data, config) -> str | None:
        pass
