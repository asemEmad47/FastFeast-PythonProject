from repository.base_repository import BaseRepository
from models.restaurant import Restaurant


class RestaurantRepository(BaseRepository[Restaurant]):
    __model__ = Restaurant
