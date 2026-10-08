"""Privacy controls applied before data leaves the restricted bronze zone.

What these do, and what they do not:

* `pseudonymise` replaces a direct identifier with a salted SHA-256 hash. That is
  pseudonymisation in the UK GDPR sense (Art. 4(5)): the output is still personal data
  and still needs protecting. The salt is a secret held in Key Vault. Without a secret
  salt, a 10-digit identifier could be recovered by brute force, because there are only
  10^10 possibilities.
* `outward_postcode` generalises a full postcode to its outward code (e.g. "MK9"), which
  covers a far larger area than a full postcode.
* Date of birth is reduced to birth year during cleaning and never stored downstream.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

# UK postcode: outward code (area + district) followed by inward code (sector + unit).
POSTCODE_REGEX = r"^([A-Z]{1,2}[0-9][A-Z0-9]?)\s?[0-9][A-Z]{2}$"


def pseudonymise(identifier: Column, salt: str) -> Column:
    normalised = F.upper(F.trim(identifier))
    return F.sha2(F.concat_ws("|", F.lit(salt), normalised), 256)


def outward_postcode(postcode: Column) -> Column:
    """Outward code of a well-formed postcode, otherwise NULL."""
    extracted = F.regexp_extract(F.upper(F.trim(postcode)), POSTCODE_REGEX, 1)
    return F.when(extracted == "", F.lit(None)).otherwise(extracted)
