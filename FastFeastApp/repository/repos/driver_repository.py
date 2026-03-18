from repository.base_repository import BaseRepository
from models.driver import Driver


class DriverRepository(BaseRepository[Driver]):
    __model__ = Driver
