"""
Task — Abstract base for all pipeline tasks.
Every Task returns (ok: bool, errors: list[str]).
"""
from abc import ABC, abstractmethod


class Task(ABC):

    @abstractmethod
    def do_task(self) -> tuple[bool, list[str]]:
        ...
