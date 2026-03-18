from repository.base_repository import BaseRepository
from models.ticket import Ticket


class TicketRepository(BaseRepository[Ticket]):
    __model__ = Ticket
