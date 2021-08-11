def construct_urls_to_business_categ(export_stage_fpath: str) -> None:
    """
        Arguments:
        * export_stage_fpath (str): Target file to export
    """
    import os
    import string
    import logging
    import pandas as pd
    from pathlib import Path

    # 1. Construction pandas dataframe of 118 business categories
    BUSINESS_CATEGORIES_URL = "http://www.118.direct/popularsearches/"
    business_categs = pd.Series(map(lambda x: f"{BUSINESS_CATEGORIES_URL}{x}",
                                    list(string.ascii_lowercase)))
    business_categs = pd.DataFrame({
        'business_categs': business_categs
    })
    # 2. Export
    fpath = os.environ['I_DIRECT_118_BUSINESS_CATEGORIES']
    fdir = fpath.rsplit('/', 1)[0]
    Path(fdir).mkdir(parents=True, exist_ok=True)
    business_categs.to_csv(fpath, index=False, compression='zip')
    logging.info(f"Business categories are exported under: {fpath}")
