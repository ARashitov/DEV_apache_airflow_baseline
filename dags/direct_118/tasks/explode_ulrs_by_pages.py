def explode_urls_over_pages(args: dict,
                            export_fpath: str) -> None:
    """
        Extracts amount of business entities for
        specific category-location

        * args: dict with next fields:
            * what (str): Business category title
            * where (str): Location
            * url (str): Full url to extract locations
    """
    import re
    import logging
    import pandas as pd
    import cloudscraper
    from bs4 import BeautifulSoup

    scrapper = cloudscraper.create_scraper()
    print(f"OUTPUT PATH: {export_fpath}")
    output_dataframe = []
    for arg in args:
        response = scrapper.get(arg['url'])

        if response.status_code == 200:

            try:

                # 1. Extraction amount of entities for
                #    specific category & location
                soup = BeautifulSoup(response.text, "html.parser")
                n_entities = soup \
                    .find_all("div", class_="resultInfoBlock")[0] \
                    .contents[0]

                # 2. Finding numeric values using regex
                n_entities = re.findall(r'\d+', n_entities.split('of ')[1])
                arg['n_entities'] = int(n_entities[0])
                logging.info(f"Succesfull finish: {arg['url']}")
            except Exception as exc:
                logging.error(f"Failure finish {arg['url']}: {str(exc)}")
                arg['n_entities'] = 0

            output_dataframe.append(arg)

        else:

            logging.error(f"Error at GET (status code): "
                          f"{response.status_code}")
            logging.error(f"Error at GET (text): {response.text}")

    output_dataframe = pd.DataFrame(output_dataframe)
    output_dataframe.to_csv(export_fpath, index=False, compression='zip')
    logging.info(f"Export dataframe to {export_fpath}")
