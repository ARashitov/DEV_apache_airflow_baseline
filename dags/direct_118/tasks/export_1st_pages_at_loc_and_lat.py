def export_1st_pages_at_loc_and_cat(http_base: str,
                                    categories_stage_dir: list,
                                    locations: list,
                                    export_fpath: str) -> None:
    """
        Generates first urls for specific location and category

        Arguments:
        * http_base (str): Endpoint of contact details
        * catcategories_stage_diregories (str): path to stage directory with
                                                business categories titles
        * locations (list<str>): List of business locations
        * export_fpath (str): Path for file export
    """
    import os
    import pandas as pd
    import logging

    # 1. Reading categories
    def read_categories(fdir: str) -> pd.Series:
        """
            Consolidates all chunks of business categories title
            to a single pandas DataFrame and retuns them
            as a single pandas Series
        """
        fpaths = fdir + pd.Series(os.listdir(fdir))

        dfs = []
        for fpath in fpaths:
            logging.info(f"Reading: {fpath}...")
            dfs.append(pd.read_csv(fpath))
        dfs = pd.concat(dfs).reset_index(drop=True)
        categories = pd.Series(dfs['business_category'])
        return categories

    categories = read_categories(categories_stage_dir)

    # 2. Building DataFrame of http requests for each location
    urls_at_loc_and_cat = []
    for location in locations:
        urls_at_loc_and_cat.append(
            pd.DataFrame({
                'what': categories,
                'where': location,
                'url': http_base + "what=" + categories + "&where=" + location,
            }))

    urls_at_loc_and_cat = pd.concat(urls_at_loc_and_cat).reset_index(drop=True)
    urls_at_loc_and_cat.to_csv(export_fpath, index=False, compression='zip')
    logging.info(f"Succesfull export intermedate parameters to {export_fpath}")
