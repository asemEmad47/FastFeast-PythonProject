from repository.base_repository import BaseRepository
from models.agent import Agent


class AgentRepository(BaseRepository[Agent]):
    __model__ = Agent
