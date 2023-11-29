from impuls.tasks import ExecuteSQL


def SetRouteColors() -> ExecuteSQL:
    return ExecuteSQL(
        task_name="SetRouteColors",
        statement=(
            "UPDATE routes SET text_color = 'FFFFFF', color = "
            "CASE short_name "
            "  WHEN 'TLK'     THEN '8505A3' "
            "  WHEN 'TLK IC'  THEN '8505A3' "
            "  WHEN 'IC'      THEN 'F25E18' "
            "  WHEN 'IC EIC'  THEN '898989' "
            "  WHEN 'IC TLK'  THEN '8505A3' "
            "  WHEN 'EC'      THEN '9D740F' "
            "  WHEN 'EIC'     THEN '898989' "
            "  WHEN 'EIC IC'  THEN '898989' "
            "  WHEN 'EIP'     THEN '002664' "
            "  WHEN 'EN'      THEN '000000' "
            "  ELSE 'DE4E4E'"
            "END"
        ),
    )
