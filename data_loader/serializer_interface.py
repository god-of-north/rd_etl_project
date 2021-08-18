import abc

class SerializerInterface(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def store_data(self, partition: str, data: dict):
        """Stroe the data set to the partition"""
        raise NotImplementedError

