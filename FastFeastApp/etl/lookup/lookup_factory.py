from __future__ import annotations
from etl.lookup.lookup            import LookUp
from etl.lookup.duplicates_lookup import DuplicatesLookUp
from etl.lookup.orphan_lookup     import OrphanLookUp
from registry.data_registry       import DataRegistry
from audit.audit                  import Audit


class LookUpFactory:

    @staticmethod
    def create_lookup(lookup_type: str, audit: Audit, registry: DataRegistry) -> LookUp:

        if lookup_type == "duplicate":
            return DuplicatesLookUp(
                audit=audit,
                registry=registry
            )
        if lookup_type == "orphan":
            return OrphanLookUp(
                audit=audit,
                registry=registry
            )
        raise ValueError(f"LookUpFactory: unknown lookup_type '{lookup_type}'")