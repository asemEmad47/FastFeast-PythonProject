"""All small lookup table repositories — one class per model."""
from repository.base_repository import BaseRepository
from models.priority       import Priority
from models.channel        import Channel
from models.region         import Region
from models.city           import City
from models.segment        import Segment
from models.team           import Team
from models.category       import Category
from models.reason         import Reason
from models.reason_category import ReasonCategory
from models.order          import Order
from models.ticket_event   import TicketEvent
from models.pipeline_run_log import PipelineRunLog


class PriorityRepository(BaseRepository[Priority]):
    __model__ = Priority

class ChannelRepository(BaseRepository[Channel]):
    __model__ = Channel

class RegionRepository(BaseRepository[Region]):
    __model__ = Region

class CityRepository(BaseRepository[City]):
    __model__ = City

class SegmentRepository(BaseRepository[Segment]):
    __model__ = Segment

class TeamRepository(BaseRepository[Team]):
    __model__ = Team

class CategoryRepository(BaseRepository[Category]):
    __model__ = Category

class ReasonRepository(BaseRepository[Reason]):
    __model__ = Reason

class ReasonCategoryRepository(BaseRepository[ReasonCategory]):
    __model__ = ReasonCategory

    def get_by_name(self, category_name: str) -> ReasonCategory:
        with self._session() as session:
            return session.query(self.__model__).filter_by(category_name=category_name).first()

class OrderRepository(BaseRepository[Order]):
    __model__ = Order

class TicketEventRepository(BaseRepository[TicketEvent]):
    __model__ = TicketEvent

class PipelineRunLogRepository(BaseRepository[PipelineRunLog]):
    __model__ = PipelineRunLog
