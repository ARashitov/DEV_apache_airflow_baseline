def add_pages_as_params_to_urls(stage_dir_fpath: str,
                                stage_export_file: str):
    """
        Adds `page` parameters to urls with taking
        into account amount of entities need to be extracted

        Arguments:
        * stage_dir_fpath (str): Fpath to directory where source
                                 data are staged.
        * stage_export_file (str): Fpath to export file
    """
    import os
    import logging
    import pandas as pd

    def read_dataframes(src_stage_fdir: str) -> pd.DataFrame:
        """
            Consolidates all chunks of business categories title
            to a single pandas DataFrame and retuns them
            as a single pandas Series

            &
        """
        fpaths = src_stage_fdir + pd.Series(os.listdir(src_stage_fdir))

        dfs = []
        for fpath in fpaths:
            logging.info(f"Reading: {fpath}...")
            dfs.append(pd.read_csv(fpath))
        dfs = pd.concat(dfs).reset_index(drop=True)
        return dfs

    def compute_n_pages_for_each_cat_loc(dfs: pd.DataFrame) -> pd.DataFrame:
        """
            Computes amount of pages needed to extract all contact
            details for specific location-category
        """
        N_ENTITIES_PER_LIST = 15

        n_int_pages = dfs['n_entities'] // N_ENTITIES_PER_LIST
        is_div_fractional = (dfs['n_entities'] % N_ENTITIES_PER_LIST) > 0
        dfs['n_pages'] = n_int_pages + is_div_fractional
        return dfs

    def generate_pages_for_urls(dfs: pd.DataFrame) -> pd.DataFrame:
        """
            Generates page number
        """
        def generate_url_with_pages(arg):
            if arg['n_pages'] > 0:
                page = pd.Series(range(1, arg['n_pages']+1, 1)).astype(str)
                output = pd.DataFrame({
                    **arg,
                    'page': page
                })
            else:
                output = pd.DataFrame()
            return output

        dfs = list(map(
            generate_url_with_pages,
            dfs.to_dict('records')
        ))
        dfs = pd.concat(dfs).reset_index(drop=True)

        return dfs

    def generate_urls_with_pages(dfs: pd.DataFrame) -> pd.DataFrame:
        dfs['url'] = dfs['url'] + "&page=" + dfs['page']
        return dfs

    def slice_dataframe(dfs: pd.DataFrame) -> pd.DataFrame:
        return dfs[['what', 'where', 'url']]

    def export_itself(dfs: pd.DataFrame) -> pd.DataFrame:
        dfs.to_csv(stage_export_file, index=False, compression='zip')

    steps = [
        read_dataframes,
        compute_n_pages_for_each_cat_loc,
        generate_pages_for_urls,
        generate_urls_with_pages,
        slice_dataframe,
        export_itself,
    ]

    dfs = stage_dir_fpath
    for step in steps:
        logging.info(f"Start: {step.__name__}()")
        dfs = step(dfs)
        logging.info(f"Finish: {step.__name__}()\n")
