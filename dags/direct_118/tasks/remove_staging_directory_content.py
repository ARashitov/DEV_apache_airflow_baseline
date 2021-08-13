def remove_staging_directory_content(fpaths: str):

    import os
    import logging

    for fpath in fpaths:
        os.system(f"rm -rf {fpath}")
        logging.info(f"Succesfull remove: {fpath}")
