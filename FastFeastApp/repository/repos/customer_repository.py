from repository.base_repository import BaseRepository
from models.customer import Customer


class CustomerRepository(BaseRepository[Customer]):
    __model__ = Customer
