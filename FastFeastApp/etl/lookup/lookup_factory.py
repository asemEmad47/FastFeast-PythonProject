"""
LookUpFactory — creates OrphanLookUp or DuplicatesLookUp by type string.
"""
from etl.lookup.orphan_lookup      import OrphanLookUp
from etl.lookup.duplicates_lookup  import DuplicatesLookUp


class LookUpFactory:

    @staticmethod
    def create_lookup(lookup_type: str, **kwargs):
        if lookup_type == "orphan":
            return OrphanLookUp(**kwargs)
        elif lookup_type == "duplicate":
            return DuplicatesLookUp(**kwargs)
        else:
            raise ValueError(f"LookUpFactory: unknown type '{lookup_type}'")
