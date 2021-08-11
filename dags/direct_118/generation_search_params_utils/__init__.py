def generate_search_parameters(http_base: str,
                               categories_stage_dir: str,
                               locations: list,
                               export_fpath: str) -> None:
    """
        Generates first urls for specific location and category

    """

    import os
    import pandas as pd
    import logging

    # 1. Reading categories
    def read_categories(fdir: str) -> pd.DataFrame:

        fpaths = fdir + pd.Series(os.listdir(fdir))

        dfs = []
        for fpath in fpaths:
            logging.info(f"Reading: {fpath}...")
            dfs.append(pd.read_csv(fpath))
        dfs = pd.concat(dfs).reset_index(drop=True)
        return dfs

    categories = read_categories(categories_stage_dir)

    # 2. Building DataFrame of http requests
    logging.info(f"Start building requrests for [{locations}]")
    http_requests = []

    for what, where in zip([categories['business_category']]*len(locations),
                           locations):
        http_requests.append(pd.DataFrame({
            'what': what, 'where': where,
            'url': http_base + "what=" + what + "&where=" + where,
        }))

    http_requests = pd.concat(http_requests).reset_index(drop=True)
    http_requests.to_csv(export_fpath, index=False, compression='zip')
