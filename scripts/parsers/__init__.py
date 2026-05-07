"""Foundation-specific parsers for awardee scraping.

Each parser exposes a ``parse()`` function that returns ``List[Dict]`` records
with keys: ``fiscal_year``, ``awardee_name``, ``awardee_affiliation``,
``awardee_position``, ``project_title``, ``award_amount``, ``program_name``,
``source_url``, ``field_category_id`` (optional), ``metadata`` (optional dict).
"""
